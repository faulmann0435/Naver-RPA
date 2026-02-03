"""
E-commerce Order Processing → Sokcho Order Form
- Password-protected Excel (.xlsx) via msoffcrypto-tool
- Smart Header Detection: skip instruction rows, find row with 상품명/수취인명/옵션정보
- CSV with encoding auto-detect; same header logic
- 13-rule option processing, post-load filter, hardcoded E/H/I
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


# ----- Smart header: row must contain all these keywords -----
HEADER_KEYWORDS = ["상품명", "수취인명", "옵션정보"]

# ----- Row filter phrase (post-load safety net) -----
FILTER_PHRASE = "다운로드 받은 파일로 '엑셀 일괄발송' 처리하는 방법"

# ----- Quantity: Column M = index 12 if not named '수량' -----
QTY_COLUMN_INDEX = 12

# ----- Hardcoded Sokcho form values -----
SENDER_NAME = "최고다농수산"
SENDER_PHONE = "033-636-0357"
SENDER_ADDRESS = "강원도 속초시 농공단지2길 15-13 다동 1층 최고다농수산"


def _strip_columns(df):
    """Strip whitespace from all column names."""
    df.columns = [str(c).strip() for c in df.columns]
    return df


def find_header_row(preview_df):
    """
    Find first row index where all HEADER_KEYWORDS appear in that row (any column).
    preview_df: DataFrame read with header=None (e.g. first 20 rows).
    Returns 0 if not found (use first row as header).
    """
    for i in range(len(preview_df)):
        row_vals = preview_df.iloc[i].astype(str).str.strip().tolist()
        row_text = " ".join(row_vals)
        if all(kw in row_text for kw in HEADER_KEYWORDS):
            return i
    return 0


def ensure_quantity_column(df):
    """Use column '수량' if present; else use Column M (index 12) and rename to 수량."""
    if "수량" in df.columns:
        return df
    if df.shape[1] <= QTY_COLUMN_INDEX:
        return df
    col = df.columns[QTY_COLUMN_INDEX]
    df.rename(columns={col: "수량"}, inplace=True)
    return df


def process_option(row):
    """
    Apply 13 rules sequentially. Uses qty_applied flag for Rule 12 (fallback).
    row must have: 수량, 옵션정보 (상품명 optional for future use).
    """
    raw_option = row.get("옵션정보", "")
    qty = row.get("수량", 1)
    try:
        qty = int(qty)
        if qty <= 0:
            qty = 1
    except (TypeError, ValueError):
        qty = 1

    if pd.isna(raw_option):
        raw_option = ""
    option = str(raw_option)

    # 1. Cleaning: prefixes ending in "선택:", emojis/special chars (keep dates), strip
    option = re.sub(r".*?(?:선택)[:]\s*", "", option)
    option = re.sub(r"[^\w\s가-힣\.\-\(\)/]", " ", option)
    option = re.sub(r"\s+", " ", option).strip()

    if not option:
        return f"{qty}개"

    # 2. Rule 6 (Special): ["명란", "배추", "와사비", "올리브오일", "과메기"] -> ({Option})*{Qty}
    special_keywords = ["명란", "배추", "와사비", "올리브오일", "과메기"]
    if any(k in option for k in special_keywords):
        return f"({option})*{qty}"

    # 3. Rule 11: Mask (...)
    paren_map = {}

    def _mask_paren(m):
        inner = m.group(0)
        key = f"__P{len(paren_map)}__"
        paren_map[key] = inner
        return key

    masked = re.sub(r"\([^)]*\)", _mask_paren, option)
    processed = masked
    qty_applied = False

    # 4. Rule 3 & 4 (Weight): ["가리비", "문어", "킹크랩", "대게", "홍게", "장어"] g->kg, * Qty
    weight_keywords = ["가리비", "문어", "킹크랩", "대게", "홍게", "장어"]
    if any(k in processed for k in weight_keywords):

        def _weight_repl(m):
            nonlocal qty_applied
            num = float(m.group(1))
            u = m.group(2).lower()
            kg = num / 1000.0 if u == "g" else num
            total_kg = kg * qty
            qty_applied = True
            return f"{int(total_kg)}kg" if total_kg.is_integer() else f"{total_kg:.1f}kg"

        processed = re.sub(r"(\d+(?:\.\d+)?)\s*(kg|KG|Kg|g|G)", _weight_repl, processed)

    # 5. Rule 5, 7, 8, 10 (Units): 두름, 병, 마리, 미
    #    마리: rewrite as {Qty}마리 (replace number). Others: number * Qty.
    def _count_repl(m):
        nonlocal qty_applied
        num_str, unit = m.group(1), m.group(2)
        base = int(num_str) if num_str else 1
        if unit == "마리":
            total = qty  # replace with order qty
        else:
            total = base * qty
        qty_applied = True
        return f"{total}{unit}"

    processed = re.sub(r"(\d+)\s*(두름|병|마리|미)", _count_repl, processed)

    def _unit_only(m):
        nonlocal qty_applied
        unit = m.group(1)
        qty_applied = True
        n = qty if unit == "마리" else (1 * qty)
        return f"{n}{unit}"

    processed = re.sub(r"\b(두름|병|마리|미)\b", _unit_only, processed)

    # 6. Rule 2 & 9 (Items): ["무침", "소스", "밀키트세트", "젓갈"] -> append {Qty}개
    item_keywords = ["무침", "소스", "밀키트세트", "젓갈"]
    extra = []
    for kw in item_keywords:
        if kw in masked:
            extra.append(f"{kw} {qty}개")
            qty_applied = True
    if extra:
        processed = (processed.strip() + " / " + " / ".join(extra)).strip()

    # 7. Rule 12 (Fallback): if no changes, append " {Qty}개"
    if not qty_applied:
        processed = processed.strip() + f" {qty}개"

    # 8. Finalize: Unmask
    for key, text in paren_map.items():
        processed = processed.replace(key, text)

    return processed.strip()


def read_csv_with_encoding(file):
    """Read CSV with encoding auto-detect and Smart Header Detection."""
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
            continue
    raise ValueError(
        f"CSV 인코딩을 판별할 수 없습니다. (utf-8, cp949, euc-kr 시도). {last_err}"
    )


def _get_excel_bytes(uploaded_file, password=None):
    """Return raw bytes (decrypt if .xlsx with password)."""
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
    """Load .xlsx with Smart Header Detection. Returns DataFrame."""
    file_name = (uploaded_file.name or "").lower()
    if not file_name.endswith(".xlsx"):
        raise ValueError("엑셀 파일(.xlsx)이 아닙니다.")

    try:
        raw_bytes = _get_excel_bytes(uploaded_file, password=password)
    except ValueError:
        raise
    except Exception as e:
        if not HAS_MSOFFCRYPTO:
            st.warning("비밀번호로 보호된 엑셀을 사용하려면: pip install msoffcrypto-tool")
        raise ValueError(f"엑셀 읽기 실패 (비밀번호 필요할 수 있음): {e}") from e

    # Smart Header: find row containing 상품명, 수취인명, 옵션정보
    preview = pd.read_excel(BytesIO(raw_bytes), header=None, nrows=20)
    header_idx = find_header_row(preview)
    df = pd.read_excel(BytesIO(raw_bytes), header=header_idx)
    _strip_columns(df)
    ensure_quantity_column(df)
    return df


def filter_instruction_rows(df):
    """Remove rows where any cell contains FILTER_PHRASE."""
    if df.empty:
        return df
    mask = df.astype(str).apply(
        lambda row: row.str.contains(FILTER_PHRASE, na=False).any(), axis=1
    )
    return df.loc[~mask].reset_index(drop=True)


def main():
    st.title("E-commerce 주문 → 속초 발주양식")

    st.write(
        "스마트스토어 주문 엑셀(.xlsx, 비밀번호 가능) 또는 CSV를 업로드하면 "
        "**Smart Header**로 상단 안내 행을 건너뛰고, 옵션 규칙을 적용해 속초 발주양식 엑셀을 생성합니다."
    )

    if not HAS_MSOFFCRYPTO:
        st.warning(
            "비밀번호 보호 엑셀을 사용하려면 터미널에서 설치: `py -m pip install msoffcrypto-tool`"
        )

    uploaded_file = st.file_uploader(
        "주문 파일 업로드 (.xlsx 또는 .csv)",
        type=["xlsx", "csv"],
    )
    if uploaded_file is None:
        return

    password = None
    if (uploaded_file.name or "").lower().endswith(".xlsx"):
        password = st.text_input(
            "엑셀 비밀번호 (없으면 비워두기)",
            type="password",
            key="excel_password",
        )

    # Load (Smart Header Detection applied inside load_excel / read_csv_with_encoding)
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

    # Post-load filtering (safety net)
    before_count = len(df)
    df = filter_instruction_rows(df)
    removed = before_count - len(df)
    if removed:
        st.info(f"안내 문구가 포함된 행 {removed}개를 제거했습니다.")

    if df.empty:
        st.warning("처리할 데이터가 없습니다.")
        return

    st.write("원본 미리보기 (상위 5행):")
    st.dataframe(df.head())

    required = ["수량", "상품명", "옵션정보", "수취인명", "구매자연락처", "통합배송지", "배송메세지"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        st.error(f"필수 컬럼 누락: {', '.join(missing)}")
        st.stop()

    st.write("옵션 규칙 적용 중...")
    df["processed_option"] = df.apply(process_option, axis=1)

    # Sokcho form: A~I with updated column order
    out = pd.DataFrame({
        "받는사람": df["수취인명"].values,
        "전화번호": df["구매자연락처"].values,
        "주소": df["통합배송지"].values,
        "구분": df["processed_option"].values,
        "수량(공란)": "",
        "배송메세지": df["배송메세지"].values,
        "보내는사람": SENDER_NAME,
        "보내는분 전화": SENDER_PHONE,
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
    st.success("처리 완료. 위 버튼으로 엑셀을 다운로드하세요.")


if __name__ == "__main__":
    main()
