"""
Collect a list of businesses for a given city.
"""
import argparse
import json
import requests

from tqdm import tqdm


MAX_LEN = 10  # max number of results per page
API_KEY = "5UCj2QJkA45jBWirU00hjLBL_geq-3PFqerB6gWhAjratq2vecmAeJsdl54AYKBLjEDlt9H_mz0K704AoUC_JyTuWH_UMBPax5pPTf1phiwfr4MzYNQS3IjU9UFpY3Yx"

def fetch(city, limit=50, offset=None):
    """Return list of restaurants."""
    endpoint = "https://api.yelp.com/v3/businesses/search"
    params = dict()
    params["location"] = city
    params["limit"] = limit
    if offset is not None:
        params["offset"] = offset

    r = requests.get(
        endpoint,
        params,
        headers={
            "Authorization": f"Bearer {API_KEY}"
        }
    )
    json_response = r.json()
    restaurants = json_response["businesses"]
    total = json_response["total"]
    return restaurants, total


def main(city, topk, outfile=None):
    if outfile is None:
        outfile = f"{city}.jsonl"

    cumulative = 0
    total = 0
    restaurants = []
    pbar = tqdm()
    while cumulative < topk or topk < 0:
        results, total = fetch(city)
        pbar.total = min(topk, total) if topk > 0 else total
        pbar.update(len(results))
        cumulative += len(results)
        restaurants += results
        if cumulative >= total:
            break
    pbar.close()
    print(f"{len(restaurants)} restaurants out of {total} total")
    with open(outfile, "w") as f:
        for r in restaurants:
            f.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("city")
    parser.add_argument("--topk", default=None, type=int)
    parser.add_argument("--outfile", default=None)

    main(**vars(parser.parse_args()))
