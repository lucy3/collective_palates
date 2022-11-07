"""
Collect a list of businesses for a given ZIP code.
"""
import argparse
import json
import requests


MAX_LEN = 10  # max number of results per page

def fetch(zip, start):
    """Return list of restaurants."""
    restaurants = []
    ENDPOINT = f"https://www.yelp.com/search/snippet?find_desc=Restaurants&find_loc={zip}&start={start}"
    r = requests.get(ENDPOINT).json()
    if "mainContentComponentsListProps" not in r["searchPageProps"]:
        return []
    for section in r["searchPageProps"]["mainContentComponentsListProps"]:
        if "bizId" in section and not section["searchResultBusiness"]["isAd"]:
            restaurants.append(section)

    return restaurants

def main(zip, topk=20, outfile=None):
    if outfile is None:
        outfile = f"{zip}.jsonl"

    restaurants = []
    all = topk == -1
    start = 0
    while start < topk or all:
        results = fetch(zip, start)
        restaurants += results
        if len(results) < MAX_LEN:
            break
        start += 10
    with open(outfile, "w") as f:
        for r in restaurants:
            f.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("zip")
    parser.add_argument("--topk", default=20, type=int)
    parser.add_argument("--outfile", default=None)

    main(**vars(parser.parse_args()))
