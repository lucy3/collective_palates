"""
Collect a list of businesses for a given city.
"""
import argparse
import json
import requests

import numpy as np
import geopandas as gpd
from shapely import vectorized

from tqdm import tqdm
import zipcodes


MAX_LEN = 10  # max number of results per page
API_KEY = "5UCj2QJkA45jBWirU00hjLBL_geq-3PFqerB6gWhAjratq2vecmAeJsdl54AYKBLjEDlt9H_mz0K704AoUC_JyTuWH_UMBPax5pPTf1phiwfr4MzYNQS3IjU9UFpY3Yx"
GRID_RES = 1000  # Distance between points on grid, in meters



def fetch(lng, lat, limit=50, offset=None, price=None):
    """Return list of restaurants."""
    endpoint = "https://api.yelp.com/v3/businesses/search"
    params = dict()
    params["longitude"] = lng
    params["latitude"] = lat
    params["radius"] = int(GRID_RES / 2 * np.sqrt(2))
    params["term"] = "restaurants"
    params["limit"] = limit
    if offset is not None:
        params["offset"] = offset
    if price is not None:
        params["price"] = price

    r = requests.get(
        endpoint,
        params,
        headers={
            "Authorization": f"Bearer {API_KEY}"
        }
    )
    try:
        json_response = r.json()
        restaurants = json_response["businesses"]
        total = json_response["total"]
        return restaurants, total
    except Exception as e:
        print("ERROR")
        print(r.text)
        print(lng, lat)
        return [], 0


def get_grid_centroids(place_name, shpfile):
    places = gpd.read_file(shpfile)
    city = places[places.NAME.eq(place_name)].to_crs(3857).iloc[0]
    geometry = city.geometry.buffer(GRID_RES / 2)
    minx, miny, maxx, maxy = geometry.bounds
    x = np.arange(minx, maxx + GRID_RES / 2, GRID_RES)
    y = np.arange(miny, maxy + GRID_RES / 2, GRID_RES)
    XX, YY = np.meshgrid(x, y)
    in_polygon = vectorized.contains(geometry, XX, YY)
    x_in_polygon = XX[in_polygon].ravel()
    y_in_polygon = YY[in_polygon].ravel()
    centroids = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(x_in_polygon, y_in_polygon, crs="EPSG:3857")
    ).to_crs(4269)
    return centroids



def main(place_name, shpfile, topk, outfile=None):
    if outfile is None:
        outfile = f"{place_name}.jsonl"

    centroids = get_grid_centroids(place_name, shpfile)

    restaurants = []
    for centroid in tqdm(centroids.geometry):
        cumulative = 0
        total = 0
        pbar = tqdm()
        while cumulative < topk or topk < 0:
            results, total = fetch(centroid.x, centroid.y, offset=cumulative)
            pbar.total = min(topk, total) if topk > 0 else total
            pbar.update(len(results))
            cumulative += len(results)
            restaurants += results
            if cumulative >= total:
                break
        pbar.close()
    seen = set()
    with open(outfile, "w") as f:
        for r in restaurants:
            if r["id"] in seen:
                continue
            f.write(json.dumps(r) + "\n")
            seen.add(r["id"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("place_name")
    parser.add_argument("shpfile")
    parser.add_argument("--topk", default=None, type=int)
    parser.add_argument("--outfile", default=None)

    main(**vars(parser.parse_args()))
