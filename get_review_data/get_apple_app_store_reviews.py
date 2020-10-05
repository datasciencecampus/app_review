"""
August 2020
David Pugh, Data Science Campus
Python code to download the latest available reviews from the App Store for a 
given App ID
The Apple Review API
Anyone can retrieve the RSS feed for a given app using : 

https://itunes.apple.com/{country}/rss/customerreviews/page={page no}/id={your app id}/sortBy=mostRecent/json


- country is the App Store country where you sell it, e.g. gb
- page no is the page of the datato returnj - data is paginated
- app id is the number following "id" in the App Store URL

The API is called using the requests library
Output is in nested JSON, an examples is given in the example json file on the repo.
Each of the nested sections is stored as a list of tuples to aid extraction.

The function will:
 - loop through the required number of pages
 - get the reviews in JSON format using teh requests library
 - save each JSON response to storage
 - extract each review from each page and collate as a large dictionary
 - save this as a CSV file
 
Example use:
    
    no_of_pages = 10
    review_id = 1234567890       
    all_reviews = get_and_collect_reviews(review_id, no_of_pages) 
    save_reviews(all_reviews, define_csv_file_name() )
"""

import json
import pandas as pd
import requests  
from datetime import datetime


"""
Define each section of the expected JSON/dict response. Each list represents 
a nest in the JSON the tuple is of the 
        (JSON Key, JSON nested key, name of extracted data)
e.g.,
			"author": {
				"uri": {
					"label": "https://itunes.apple.com/gb/reviews/id419600570"
				},
				"name": {
					"label": "name1"
				},
			}
is captured as 
author_keys = [
        ("uri","label", "author_uri"),
        ("name","label", "author_name")
        ] 
and so 
author->name->label will be extracted and stored as author_name
author->uri->label will be extracted and stored as author_uri in a dict:
{ 
    "author_uri" : "https://itunes.apple.com/gb/reviews/id419600570",
    "author_name" : "name1"
}
"""


author_keys = [
        ("uri","label", "author_uri"),
        ("name","label", "author_name")
        
        ]  

other_keys = [
        ("im:version","label", "im_version"),
        ("im:rating","label", "im_rating"),
        ("id","label", "id"),
        ("title","label", "title"),
        ("content","label","content"),
        ("im:voteSum","label", "im_votesum"),
        ("im:voteCount","label", "im_votecount"), 
        ] 

link_keys = [
        ( 'attributes','rel', 'link_attributes_related'),
        ( 'attributes', 'href','link_attributes_href')
        ] 

content_keys = [
        ( 'attributes','term', 'content_attributes_term'),
        ( 'attributes','label','content_attributes_label')
        ] 

review_sections = [
        ('author', author_keys),
        ('link', link_keys),
        ('im:contentType', content_keys)
        ]
  

def get_and_collect_reviews(review_id, no_of_pages):
    """ gets reviews from the Apple API for a given App ID, and processes the
        JSON response to create a flattened table
    
    Args:
        review_id (int) - the ID of the app 
        no_of_pages (int) - the total number of pages of reviews to return
    
    Returns: 
        a list of dicts, each dict being a flattened review entry
    
    """
    all_reviews = [] 
    # for all pages
    for page in range(no_of_pages):
    
        # get the JSON response from the API fopr that page
        reviews_response = get_reviews(review_id, page+1)
        # Check to see if we have any reviews in the response
        if reviews_response is None:
            print("Cannot get reviews")
            return
        else:
            # Load the JSON response to dict
            reviews = json.loads(reviews_response.text)
            # Get the review list within the dict
            try:
                review_list = reviews['feed']['entry']
                # Save the review information in raw JSON 
                save_json(reviews_response.text, page+1)
                # process the reviews and add the reviews to our list
                all_reviews = process_reviews(all_reviews, review_list)
            except Exception as e:    
                print("No more entries")
                break
    
    return (all_reviews)

def get_reviews(review_id, page_no):
    """ 
    Get the reviews from the Apple APIfor a given App using the requests package 
    
    Args:
        review_id (int) - the id of the Apple App ID you want the reviews from
        page_no (int) - data is paginated, so this dermines which page number 
        to return
    Returns:
        the response from the api (if status == 200), otherwise None
    
    """
    
    print(f"Getting reviews from Apple API for page {page_no}")
    url = f'https://itunes.apple.com/gb/rss/customerreviews/page={page_no}/id={review_id}/sortBy=mostRecent/json'
    response = requests.get(url)
    if response.status_code == 200:
        return response
    elif response is None:
        return None
    else:
        print(f"Error retrieving reviews {response.status_code}")
        return None


def define_csv_file_name():
    # defines a timestamped filename for the flattened table data
    ts = datetime.now()
    file_name = f"apple_review_{ts.strftime('%Y%m%d_%H%M%S')}.csv"
    return file_name


def define_json_file_name(page):
    # defines a timestamped filename for the flattened table data
    ts = datetime.now()
    file_name = f"apple_review_page_{page}_{ts.strftime('%Y%m%d_%H%M%S')}.json"
    return file_name


def process_reviews(all_reviews, review_list): 
    """ Process the JSON reviews 
    These are nested so we use the list of tuples defined within the Cloud Function to navigate the JSON structure
    and extract useful data to produce a flattended dictionary of review data
    other_keys - these are the main keys in the JSON file and include the OS type, review text
    review_sections are the other nested sections and include author details
    Args:
        all_reviews (list of dict) - list of all currently extracted reviews
        review_list (list of dict) - list of new reviews to be extracted
    Returns:
        list of extracted data fro review_list, appended to all_reviews
    """
    print("Processing the JSON response")
    for review in review_list:
        review_flat= extract_matches(other_keys, review)
        for section in review_sections:
            review_flat.update(extract_matches(section[1], review[section[0]]))
        all_reviews.append(review_flat)  
    return all_reviews  

def extract_matches(keys, review_section):
    """ Extracts the given keys from the given section of the reviews and saves in a flattened dictionary
    
    Args:
        keys (tuple of (list of tuples)) - the keys to extract for a particular nest of the 
        JSON  e.g., [("uri","label", "author_uri"),("name","label", "author_name")]) 
        review_section (dict) - the section of JSON response to extract data from, eg., 
        {"author": {"uri": {"label": "https://itunes.apple.com/gb/reviews/id417600570"},"name": {"label": "name1"}
    Returns:
        dict of extracted data
    """
    review_extract = {}
    for entry in review_section:
        for key in keys:
            if entry == key[0]:
                if key[1]== '':
                    review_extract[key[2]] = review_section[entry]
                else:    
                    review_extract[key[2]] = review_section[entry][key[1]]
    return review_extract        

           
def save_reviews(all_reviews, file_name):
    # Convert to pandas dataframe
    df = pd.DataFrame(all_reviews)
    # Add time column
    ts = datetime.now()
    df['date'] = ts.strftime('%Y-%m-%dT%H:%M%:%SZ')
    # Save to csv
    df.to_csv(file_name, index=False)
    print(f"All reviews flattened and saved to {file_name}")

 
def save_json(text, page_no):
    # Save response to JSON file
    file_name = define_json_file_name(page_no)
    with open(file_name, "w") as json_file:
        print(f"{text}", file=json_file)
    print(f"JSON response saved to {file_name}")
    
