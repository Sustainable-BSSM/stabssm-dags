import polars as pl

_WANTED_BASE = "https://www.wanted.co.kr/wd"
_LINK_COLOR = "#0066cc"


def build_job_postings_section(df: pl.DataFrame) -> dict:
    """
    골드 채용 공고 DataFrame → 뉴스레터 섹션 dict.
    반환 형식은 ArticleRewriter.write_section() 반환값과 동일.
    """
    if df.is_empty():
        return {}

    sections = []
    for row in df.to_dicts():
        company = row["company_name"]
        position = row["position"]
        district = row["district"]
        employment = "정규직" if row["employment_type"] == "regular" else "인턴"

        tags = [f"서울 {district}", employment]
        if row.get("is_newbie"):
            tags.append("신입 가능")
        if row.get("is_alternative_military"):
            tags.append("병역특례")
        annual_to = row.get("annual_to") or 0
        if annual_to == 100:
            tags.append("경력 무관")
        elif annual_to >= 10:
            tags.append(f"경력 {annual_to}년 이하")

        tag_str = " · ".join(tags)
        link = f"{_WANTED_BASE}/{row['id']}"

        body = (
            f"{position}<br/>"
            f'<font color="#888888">{tag_str}</font><br/>'
            f'<a href="{link}"><font color="{_LINK_COLOR}">지원하기 →</font></a>'
        )

        sections.append({"title": company, "body": body})

    return {"sections": sections, "references": []}
