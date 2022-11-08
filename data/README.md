# Datasets

This is where datasets are stored.

## Downloading prepared datasets
Instructions tk

## Fetching data using data collection scripts
This is a two-step process. First, we fetch a list of businesses in a given
city. Then, we fetch reviews for those businesses. In the project directory,
run:

```sh
python data-collection/collect_businesses.py "San Francisco" ./data-collection/shapefiles/CityBoundaries.shp --topk=-1 --outfile=./data/SanFrancisco.jsonl

python data-collection/collect_reviews.py ./data/SanFrancisco.jsonl --topk=20 --num-restaurants=-1 --outfile=./data/SanFrancisco_Reviews.jsonl
```
