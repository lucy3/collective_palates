import argparse
import ujson as json

import requests
from tqdm import tqdm



def fetch(id, session, limit=20):
    endpoint = f"https://www.yelp.com/biz/{id}/review_feed?&sort_by=relevance_desc"
    params = dict()
    params["sort_by"] = "relevance_desc"

    cumulative = 0
    reviews = []

    pbar = tqdm()
    while cumulative < limit or limit < 0:
        r = session.get(endpoint, data=params)
        json_response = r.json()
        results = json_response["reviews"]
        total = json_response["pagination"]["totalResults"]
        reviews += results

        pbar.total = min(limit, total) if limit > 0 else total
        pbar.update(len(results))

        cumulative += len(results)
        if cumulative >= total:
            break
    pbar.close()
    return reviews


def main(businesses, topk, num_restaurants):
    session = requests.Session()
    out = open("reviews.jsonl", "w")
    for i, line in tqdm(enumerate(open(businesses, "r"))):
        if i >= num_restaurants and num_restaurants > 0:
            break
        results = fetch(json.loads(line)["id"], session, topk)
        out.write("\n".join([json.dumps(r) for r in results]) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("businesses")
    parser.add_argument("--topk", default=20, type=int)
    parser.add_argument("--num-restaurants", default=20, type=int)
    main(**vars(parser.parse_args()))
