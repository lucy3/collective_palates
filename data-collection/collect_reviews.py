import argparse
import ujson as json

import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from threading import local


thread_local = local()

def get_session() -> requests.Session:
    if not hasattr(thread_local,'session'):
        thread_local.session = requests.Session()
    return thread_local.session


def fetch(id, limit=20):
    session = get_session()
    endpoint = f"https://www.yelp.com/biz/{id}/review_feed"
    params = dict()
    params["sort_by"] = "relevance_desc"

    cumulative = 0
    reviews = []

    # pbar = tqdm()
    while cumulative < limit or limit < 0:
        params["start"] = cumulative
        r = session.get(endpoint, params=params)
        try:
            json_response = r.json()
            results = json_response["reviews"]
            total = json_response["pagination"]["totalResults"]
            reviews += results

        # pbar.total = min(limit, total) if limit > 0 else total
        # pbar.update(len(results))

            cumulative += len(results)
            if cumulative >= total:
                break
        except Exception as e:
            print(r)
            print(endpoint)
            print(params)
            print(e)
    # pbar.close()
    return reviews


def main(businesses, topk, outfile, num_restaurants):
    if outfile is None:
        out = open("reviews.jsonl", "w")
    else:
        out = open(outfile, "w")
    business_ids = []
    for i, line in tqdm(enumerate(open(businesses, "r"))):
        if i >= num_restaurants and num_restaurants > 0:
            break
        business_ids.append(json.loads(line)["id"])

    with ThreadPoolExecutor(max_workers=10) as executor:
        for results in tqdm(executor.map(lambda id: fetch(id, limit=topk), business_ids), total=len(business_ids)):
            out.write("\n".join([json.dumps(r) for r in results]) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("businesses")
    parser.add_argument("--topk", default=20, type=int)
    parser.add_argument("--outfile", default=None)
    parser.add_argument("--num-restaurants", default=20, type=int)
    main(**vars(parser.parse_args()))
