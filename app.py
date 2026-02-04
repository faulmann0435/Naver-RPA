"""
Sokcho Order Processing
- Password-protected Excel (msoffcrypto), CSV with encoding auto-detect
- Smart Header Detection, instruction row filter
- process_option (refined rules, 10마리 fix), MERGE same orders, sort by 결제일, Excel export
"""
import re
from io import BytesIO

import pandas as pd
import streamlit as st

try:
    import msoffcrypto
    HAS_MSOFFCRYPTO = True
except ImportError:
    HAS_MSOFFCRYPTO = False


HEADER_KEYWORDS = ["상품명", "수취인명", "옵션정보"]
FILTER_PHRASE = "다운로드 받은 파일로 '엑셀 일괄발송' 처리하는 방법"
QTY_COLUMN_INDEX = 12

SENDER_NAME = "최고다농수산"
SENDER_PHONE = "033-636-0357"
SENDER_ADDRESS = "강원도 속초시 농공단지2길 15-13 다동 1층 최고다농수산"


def _strip_columns(df):
    df.columns = [str(c).strip() for c in df.columns]
    return df


def find_header_row(preview_df):
    for i in range(len(preview_df)):
        row_vals = preview_df.iloc[i].astype(str).str.strip().tolist()
        row_text = " ".join(row_vals)
        if all(kw in row_text for kw in HEADER_KEYWORDS):
            return i
    return 0


def ensure_quantity_column(df):
    if "수량" in df.columns:
        return df
    if df.shape[1] <= QTY_COLUMN_INDEX:
        return df
    df.rename(columns={df.columns[QTY_COLUMN_INDEX]: "수량"}, inplace=True)
    return df


def process_option(row):
    """
    Refined rules: Pre-cleaning, 장어 handling, 마리 fix, 멍게/units/special, item suffixes, fallback.
    Uses row['상품명'], row['옵션정보'], row['수량'].
    """
    product = str(row.get("상품명", "") or "")
    raw_option = row.get("옵션정보", "")
    try:
        qty = int(row.get("수량", 1))
        if qty <= 0:
            qty = 1
    except (TypeError, ValueError):
        qty = 1

    if pd.isna(raw_option):
        raw_option = ""
    text = str(raw_option).strip()

    # ----- Step 1: Pre-Cleaning (Rules 5, 6, 7) -----
    text = re.sub(r"^\d+(?:-\d+)?[\.\)]\s*", "", text)
    for w in ["특가", "사이즈"]:
        text = text.replace(w, "")
    if "국산참가리비" in product:
        text = text.replace("참가리비", "")
    for prefix in ["선택:", "상품 선택:", "중량:", "추가상품:"]:
        text = text.replace(prefix, "")
    text = re.sub(r"[^\w\s가-힣\.\-\(\)/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    if not text:
        return f"{qty}개"

    qty_applied = False

    # ----- Step 2: Specific Item - Eel (장어) -----
    if "장어" in text or "장어" in product:
        text = re.sub(r"\(.*?\)", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        # 500g exception: keep as "500g", do not convert to kg
        text = text.replace("500g", "__500G__")
        def _g_to_kg(m):
            nonlocal qty_applied
            num = float(m.group(1))
            u = m.group(2).lower()
            if u == "g":
                kg = num / 1000.0
            else:
                kg = num
            total = kg * qty
            qty_applied = True
            return f"{int(total)}kg" if total == int(total) else f"{total:.1f}kg"
        text = re.sub(r"(\d+(?:\.\d+)?)\s*(kg|KG|Kg|g|G)", _g_to_kg, text)
        text = text.replace("__500G__", "500g")

    # ----- Step 3: Unit Calculation - 마리 (Critical Fix) -----
    def _mari_repl(m):
        nonlocal qty_applied
        n = int(m.group(1)) * qty
        qty_applied = True
        return f"{n}마리"

    text = re.sub(r"(\d+)\s*마리", _mari_repl, text)

    # ----- Step 4: 멍게 (Rule 8), Standard Units, Special -----
    if "멍게" in text:
        if re.search(r"\d+\s*kg", text):
            qty_applied = True

    for unit in ["두름", "병", "미"]:
        def _unit_repl(m, u=unit):
            nonlocal qty_applied
            num_str = m.group(1)
            base = int(num_str) if num_str else 1
            total = base * qty
            qty_applied = True
            return f"{total}{u}"
        text = re.sub(r"(\d+)\s*" + re.escape(unit), _unit_repl, text)

    special_keywords = ["명란", "배추", "와사비", "올리브오일", "과메기"]
    if any(k in text for k in special_keywords):
        text = f"({text})*{qty}"
        qty_applied = True
        return text.strip()

    # ----- Step 5: Item Suffixes (Rule 1) -----
    item_keywords = ["무침", "소스", "젓갈"]
    extra = []
    for kw in item_keywords:
        if kw in text:
            extra.append(f"{kw} {qty}개")
            qty_applied = True
    if extra:
        text = (text.strip() + " / " + " / ".join(extra)).strip()
        text = re.sub(r"\s*/\s*무침\s*$", "", text)
        text = re.sub(r"무침\s*1개", "무침", text)

    # ----- Step 6: Fallback -----
    if not qty_applied:
        text = text.strip() + f" {qty}개"

    return text.strip()


def read_csv_with_encoding(file):
    encodings = ("utf-8-sig", "utf-8", "cp949", "euc-kr")
    last_err = None
    for enc in encodings:
        try:
            file.seek(0)
            preview = pd.read_csv(file, encoding=enc, header=None, nrows=20)
            file.seek(0)
            header_idx = find_header_row(preview)
            file.seek(0)
            df = pd.read_csv(file, encoding=enc, header=header_idx)
            _strip_columns(df)
            ensure_quantity_column(df)
            return df
        except Exception as e:
            last_err = e
    raise ValueError(f"CSV 인코딩 판별 실패. {last_err}")


def _get_excel_bytes(uploaded_file, password=None):
    raw = uploaded_file.read()
    if not raw:
        raise ValueError("파일 내용이 비어 있습니다.")
    if not (password and password.strip() and HAS_MSOFFCRYPTO):
        return raw
    try:
        bio = BytesIO(raw)
        office_file = msoffcrypto.OfficeFile(bio)
        office_file.load_key(password=password.strip())
        decrypted = BytesIO()
        office_file.decrypt(decrypted)
        decrypted.seek(0)
        return decrypted.getvalue()
    except Exception as e:
        err_msg = str(e).lower()
        if "invalidkey" in type(e).__name__.lower() or "password" in err_msg or "decrypt" in err_msg:
            raise ValueError("비밀번호가 올바르지 않습니다.") from e
        raise ValueError(f"암호 해제 실패: {e}") from e


def load_excel(uploaded_file, password=None):
    file_name = (uploaded_file.name or "").lower()
    if not file_name.endswith(".xlsx"):
        raise ValueError("엑셀 파일(.xlsx)이 아닙니다.")
    try:
        raw_bytes = _get_excel_bytes(uploaded_file, password=password)
    except ValueError:
        raise
    except Exception as e:
        if not HAS_MSOFFCRYPTO:
            st.warning("비밀번호 보호 엑셀: pip install msoffcrypto-tool")
        raise ValueError(f"엑셀 읽기 실패: {e}") from e
    preview = pd.read_excel(BytesIO(raw_bytes), header=None, nrows=20)
    header_idx = find_header_row(preview)
    df = pd.read_excel(BytesIO(raw_bytes), header=header_idx)
    _strip_columns(df)
    ensure_quantity_column(df)
    return df


def filter_instruction_rows(df):
    if df.empty:
        return df
    mask = df.astype(str).apply(
        lambda row: row.str.contains(FILTER_PHRASE, na=False).any(), axis=1
    )
    return df.loc[~mask].reset_index(drop=True)


def merge_orders(df):
    """
    Group by 수취인명, 수취인연락처1 (or 구매자연락처), 통합배송지.
    Aggregate: processed_option join " / ", 배송메세지 unique join " / ", 구매자명 first, 결제일 min.
    """
    phone_col = "수취인연락처1" if "수취인연락처1" in df.columns else "구매자연락처"
    if phone_col not in df.columns:
        raise ValueError("전화번호 컬럼 없음: 수취인연락처1 또는 구매자연락처 필요")
    group_cols = ["수취인명", phone_col, "통합배송지"]
    for c in group_cols:
        if c not in df.columns:
            raise ValueError(f"병합 키 컬럼 없음: {c}")

    def join_options(ser):
        vals = ser.dropna().astype(str).str.strip()
        return " / ".join(v for v in vals if v)

    def join_unique_messages(ser):
        parts = ser.dropna().astype(str).str.strip().unique().tolist()
        return " / ".join(p for p in parts if p)

    agg_dict = {
        "processed_option": join_options,
        "배송메세지": join_unique_messages,
        "구매자명": "first",
    }
    if "결제일" in df.columns:
        agg_dict["결제일"] = "min"

    merged = df.groupby(group_cols, as_index=False).agg(agg_dict)
    return merged


def sort_by_payment_date(df):
    """Sort DataFrame by 결제일 ascending (oldest first). NaT/NaN last."""
    if "결제일" not in df.columns or df.empty:
        return df
    s = pd.to_datetime(df["결제일"], errors="coerce")
    df = df.copy()
    df["_sort_date"] = s
    df = df.sort_values("_sort_date", ascending=True, na_position="last").drop(columns=["_sort_date"])
    return df.reset_index(drop=True)


def main():
    st.title("속초 발주 처리")

    st.write(
        "주문 엑셀(.xlsx, 비밀번호 가능) 또는 CSV 업로드 → Smart Header, 옵션 규칙 적용, "
        "동일 주소 병합·결제일 기준 정렬 후 속초 발주양식 엑셀 생성."
    )

    if not HAS_MSOFFCRYPTO:
        st.warning("비밀번호 엑셀: `py -m pip install msoffcrypto-tool`")

    uploaded_file = st.file_uploader("주문 파일 (.xlsx 또는 .csv)", type=["xlsx", "csv"])
    if uploaded_file is None:
        return

    password = None
    if (uploaded_file.name or "").lower().endswith(".xlsx"):
        password = st.text_input("엑셀 비밀번호 (없으면 비움)", type="password", key="pw")

    file_name = (uploaded_file.name or "").lower()
    try:
        if file_name.endswith(".xlsx"):
            df = load_excel(uploaded_file, password=password)
        else:
            uploaded_file.seek(0)
            df = read_csv_with_encoding(uploaded_file)
    except Exception as e:
        st.error(str(e))
        return

    before = len(df)
    df = filter_instruction_rows(df)
    if before > len(df):
        st.info(f"안내 문구 행 {before - len(df)}개 제거.")

    if df.empty:
        st.warning("처리할 데이터가 없습니다.")
        return

    st.write("원본 미리보기 (상위 5행):")
    st.dataframe(df.head())

    required = ["수량", "상품명", "옵션정보", "수취인명", "통합배송지", "배송메세지"]
    phone_ok = "수취인연락처1" in df.columns or "구매자연락처" in df.columns
    if not phone_ok:
        required.append("수취인연락처1 또는 구매자연락처")
    missing = [c for c in required if c not in df.columns and "수취인연락처" not in c]
    if missing or not phone_ok:
        st.error(f"필수 컬럼 누락: {missing or '수취인연락처1/구매자연락처'}")
        st.stop()

    if "구매자명" not in df.columns:
        df["구매자명"] = ""

    st.write("옵션 규칙 적용 중...")
    df["processed_option"] = df.apply(process_option, axis=1)

    st.write("동일 수취인·주소 기준 병합 중...")
    try:
        merged = merge_orders(df)
    except Exception as e:
        st.error(str(e))
        st.stop()

    if "결제일" in merged.columns:
        st.write("결제일 기준 오름차순 정렬 중...")
        merged = sort_by_payment_date(merged)
    else:
        st.info("결제일 컬럼이 없어 정렬하지 않았습니다. 결제일이 있으면 오래된 주문부터 정렬됩니다.")

    phone_col = "수취인연락처1" if "수취인연락처1" in merged.columns else "구매자연락처"
    out = pd.DataFrame({
        "받는사람": merged["수취인명"].values,
        "전화번호": merged[phone_col].values,
        "주소": merged["통합배송지"].values,
        "구분": merged["processed_option"].values,
        "보내는사람": SENDER_NAME,
        "배송메시지": merged["배송메세지"].values,
        "(공란)": "",
        "전화번호(확인)": SENDER_PHONE,
        "보내는분주소(전체, 분할)": SENDER_ADDRESS,
    })

    st.subheader("속초 발주양식 미리보기 (상위 5행)")
    st.dataframe(out.head())

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        out.to_excel(writer, index=False, sheet_name="발주양식")
    buffer.seek(0)

    st.download_button(
        label="속초 발주양식.xlsx 다운로드",
        data=buffer,
        file_name="속초 발주양식.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    st.success("처리 완료. 엑셀을 다운로드하세요.")


if __name__ == "__main__":
    main()
