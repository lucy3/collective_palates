import json
import multiprocessing
import spacy
from tqdm import tqdm
import pandas

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
    
    doc = nlp(text)
    adj = set()
    for token in doc: 
        if token.tag.startswith('JJ'): 
            adj.add(token.text.lower())
    
    return business_id, month_year, list(adj), stars

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
        
def read_in_chunks(file_object, chunk_size=500000):
    """
    Lazy function (generator) to read a file piece by piece.
    """
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

def main():
    all_results = {
        'business_id': [],
        'month_year': [],
        'adj': [],
        'stars': [],
    }
    with open('yelp_academic_dataset_review.json', 'r') as infile: 
        for chunk in read_in_chunks(infile):
            with multiprocessing.Pool(processes=multiprocessing.cpu_count() // 3) as p:
                results = list(tqdm(p.imap(process_line, chunk), total=len(chunk)))
                for res in results: 
                    if res is None: continue
                    business_id, month_year, adj, stars = res
                    all_results['business_id'].append(business_id)
                    all_results['month_year'].append(month_year)
                    all_results['adj'].append(adj)
                    all_results['stars'].append(stars)
    df = pd.DataFrame(data=all_results)
    df.to_parquet('adjectives.csv')  

if __name__ == "__main__":
    businesses_to_keep = []
    with open('businesses_to_keep.txt', 'r') as infile:
        for line in infile: 
            businesses_to_keep.append(line.strip())
        
    nlp = spacy.load('en_web_core_sm', disable=['parser', 'ner'])   
    
    main()
