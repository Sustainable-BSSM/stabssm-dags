import json

from src.infra.crawler.bumawiki.accident import BumawikiAccidentCrawler
from src.infra.requester.http import HttpRequester


def run_job():
    requester = HttpRequester()
    crawler = BumawikiAccidentCrawler(requester=requester)
    crawled_data = crawler.run()

    titles = [item['title'] for items in crawled_data.values() for item in items]
    print(json.dumps(titles))


if __name__ == "__main__":
    run_job()
