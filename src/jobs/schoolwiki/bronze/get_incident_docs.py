import json

from src.core.schoolwiki.model import SchoolwikiCategory
from src.infra.crawler.schoolwiki.docs import SchoolwikiDocsCrawler
from src.infra.requester.http import HttpRequester


def run_job():
    requester = HttpRequester()
    crawler = SchoolwikiDocsCrawler(requester=requester, category=SchoolwikiCategory.INCIDENT)
    crawled_data = crawler.run()

    docs = [
        {"id": item["id"], "slug": item["slug"], "title": item["title"],
         "category": item["category"], "year": item.get("year")}
        for items in crawled_data.values() for item in items
    ]
    print(json.dumps(docs, ensure_ascii=False))


if __name__ == "__main__":
    run_job()
