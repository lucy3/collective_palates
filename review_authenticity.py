import json
import multiprocessing
import spacy
from tqdm import tqdm
import pandas as pd

def process_line(line): 
    '''
    @input: 
    - a json representing a review
    @outputs: 
    - business_id: string
    - month_year: string formatted as YYYY-MM
    - authentic_mention: True or False
    - stars: float
    '''
    d = json.loads(line.strip())
    business_id = d['business_id']
    if business_id not in businesses_to_keep: 
        return None
    stars = d['stars']
    month_year = d['date'][:7]
    text = d['text']
    
    authenticity_words = ["authentic", "authenticity", "legitimate", "traditional", "Americanized"]
    if business_id in business_regions: 
        regions = business_regions[business_id]
        authenticity_words += regions
        
    authenticity_words = set([w.lower() for w in authenticity_words])
    
    doc = nlp(text)
    tokens = set([token.text.lower() for token in doc])
    if tokens & authenticity_words: 
        authentic_mention = True
    else: 
        authentic_mention = False
    
    return business_id, month_year, authentic_mention, stars

def read_in_chunks(file_object, chunk_size=500000):
    """
    Lazy function (generator) to read a file piece by piece.
    """
    while True:
        data = []
        for i in range(chunk_size): 
            line = file_object.readline()
            if line.strip() == '': break
            data.append(line)
        if not data:
            break
        yield data

def main():
    all_results = {
        'business_id': [],
        'month_year': [],
        'authentic_mention': [],
        'stars': [],
    }
    with open('yelp_academic_dataset_review.json', 'r') as infile: 
        for chunk in read_in_chunks(infile):
            with multiprocessing.Pool(processes=multiprocessing.cpu_count() // 3) as p:
                results = list(tqdm(p.imap(process_line, chunk), total=len(chunk)))
                for res in results: 
                    if res is None: continue
                    business_id, month_year, authentic_mention, stars = res
                    all_results['business_id'].append(business_id)
                    all_results['month_year'].append(month_year)
                    all_results['authentic_mention'].append(authentic_mention)
                    all_results['stars'].append(stars)
    df = pd.DataFrame(data=all_results)
    df.to_parquet('authenticity_mentions.csv')

if __name__ == "__main__":
    businesses_to_keep = []
    with open('businesses_to_keep.txt', 'r') as infile:
        for line in infile: 
            businesses_to_keep.append(line.strip())
            
    with open('business_regions.json', 'r') as infile: 
        business_regions = json.load(infile)
        
    nlp = spacy.load('en_core_web_sm', disable=['parser', 'ner'])   
    
    main()
