"""
August 2020
David Pugh, Data Science Campus
Processes the raw JSON Play Store review file

Returned JSON from the API is in nested JSON, with some optional values. 
See the following link for the schema:
https://developers.google.com/android-publisher/api-ref/rest/v3/reviews 

Access to the API is controlled through oauth2 and you will need to authenticate
to access the console automatically to download reviews. For example:
    from google.oauth2 import service_account
    from apiclient.discovery import build
    try:
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    except Exception as e:
         print(e) 
         return   
    # Get list of reviews using googleapiclient.discovery
    try:      
        service = googleapiclient.discovery.build('androidpublisher', 'v3', credentials=credentials)
        response = service.reviews().list(packageName='uk.organisation.appname.production').execute()
    except Exception as e:
         print(e) 
         return
  
    if not 'reviews' in response or len(response['reviews']) == 0:
        print('No reviews')
    else:
        print('Reviews detected.')
        ....
        
The following code assumes that the reviews have been downloaded and saved to
JSON, and this has been loaded to a variable reviews_j

all_review_data = process_json(review_j)

# You can save the reviews to CSV file directly
save_reviews(all_review_data, define_csv_file_name())

# Or convert to a Pandas dataframe abnd manipluate as required
all_reviews = pd.DataFrame(all_review_data)

"""
import pandas as pd
from datetime import datetime

def process_json(review_j):
    """
    Processes the nested JSON reviews (converted to dict) to a list of flat dict 
    Each review is processed to create a flat dict of data
    Args:
        review_j (dict) - the JSON reviews convert to dict
    Returns:
        list of dict of extracted review data
    """
    # Set up a list to 
    all_review_data = []

    # Iterate through all review entries
    for entries in review_j:
        if entries == 'reviews':
            reviews = review_j[entries]
            
            # for an individual review there could be multiple comments
            # Find the author and id and then create an entry for each comment
            for review in reviews:
                author_name = None
                review_key = None
                for review_key in review:

                    # get the author name and id
                    if review_key == 'reviewId':    
                        review_id = review[review_key]
                    if review_key == 'authorName':
                        author_name = review[review_key] 
                    # Extract all comments    
                    if review_key == 'comments':
                        # Create a new entry for each user comment
                        comments = review[review_key]
                        for comment in comments:
                            review_data = extract_comments(comment, review_id, author_name)
                            all_review_data.append(review_data) 

    print(f"Processed {len(all_review_data)} records")                             
    return all_review_data


def extract_timestamp(last_modified):
    # Extracts time stamp from 
    seconds = None
    nanos = None
    for t in last_modified:
        if t == "seconds": 
            seconds = last_modified[t]
        if t == "nanos": 
            nanos = last_modified[t]
    return (seconds, nanos)  


def extract_comments(comment, review_id, author_name):
    """
    Extracts a single review from the nested dict to create a flattened 
    dict. 
    The JSON schema is outlined in https://developers.google.com/android-publisher/api-ref/rest/v3/reviews#Review
    Args:
        comment (string) - user comment for this review
        review_id (string) - review id for this review
        author_name (string) - author name for this review
    Returns:
        dict of extracted flattened data    
    """
    review_data = {} 
    # Some entries may be missing, so add them as None
    review_data['review_id'] = review_id
    review_data['author_name'] = author_name
    review_data["android_os_version"] = None
    review_data["app_version_code"] = None
    review_data["app_version_name"] = None
    review_data["device"] = None
    review_data["reviewer_language"] = None
    review_data['dev_comment_last_modified_seconds'] = None
    review_data['dev_comment_last_modified_nanos'] = None   
    review_data['dev_comment_text'] = None

    # Define the expected keys from the Review schema
    # THis is a list of tuples, the first value being the key
    # and the second is the name to be used in the flattened
    # dict file

    comments_keys =[
        ("starRating", "star_rating"),
        ("reviewerLanguage", "reviewer_language"),
        ("device", "device"),
        ("androidOsVersion", "android_os_version"),
        ("appVersionCode", "app_version_code"),
        ("appVersionName", "app_version_name"),
        ("thumbsUpCount", "thumbs_up_count"),
        ("thumbsDownCount", "thumbs_down_count"),
        ("originalText", "original_text")
    ]

    metadata_keys = [
        ("productName", "device_product_name"),
        ("manufacturer", "device_manufacturer"),
        ("screenHeightPx", "device_screen_height_px"),
        ("screenWidthPx", "device_screen_width_px"),
        ("screenHeightPx","device_screen_height_px"),
        ("nativePlatform", "device_native_platform"),
        ("screenDensityDpi", "device_screen_density_dpi"),
        ("glEsVersion", "device_gles_version"),
        ("cpuModel", "device_cpu_model"),
        ("cpuMake", "device_cpu_make"),
        ("ramMb", "device_ram_mb")     
    ]

    # The raw dict is nested, so we will process each nest section
    # seperately, e.g., nested sections include comments and metadata
    # We move through the dictionary and look for specific keys to either
    # extract directly or to identify as a nested section for extraction

    # For each use comment extract the user comments and the dev comments
    for entry in comment:  
        if entry == 'userComment':
            # This the user comments section
            user_comment = comment[entry]
            for val in user_comment:
                #print(val)
                if val == 'text':
                    review_data['user_comment'] = user_comment['text']
                elif val == 'lastModified':   
                    # Extract the timestamp  
                    s,n = extract_timestamp(user_comment['lastModified'])
                    review_data['user_comment_last_modified_seconds'] = s
                    review_data['user_comment_last_modified_nanos'] = n
                elif val == 'deviceMetadata': 
                    for n in metadata_keys:
                        review_data[n[1]] = extract_values(user_comment, n[0])   
                else:
                    # Extract the comment values
                    for n in comments_keys:
                        review_data[n[1]] = extract_values(user_comment, n[0]) 

        if entry == 'developerComment':
            
            # Developer comments
            dev_comment = comment[entry]
            for val in dev_comment:
                #print(val)
                # Extract the time stamp
                if val == 'lastModified':
                    s,n = extract_timestamp(dev_comment['lastModified'])
                    review_data['dev_comment_last_modified_seconds'] = s
                    review_data['dev_comment_last_modified_nanos'] = n
                # Extract the text    
                elif val == 'text':    
                    review_data['dev_comment_text'] = dev_comment['text']
          
    return review_data          


def extract_values(obj, key):
    """Pull value of specified key from nested dict section.
    Args:
        obj (dict) - the dict to search through
        key (string) - key to extract
    Returns:
        value asscoiated with the      
    """
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
                    return arr
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    results = extract(obj, arr, key)
    if len(results) == 0:
        return None
    else:
        return results[0]

   
def define_csv_file_name():
    # defines a timestamped filename for the flattened table data
    ts = datetime.now()
    file_name = f"google_review_{ts.strftime('%Y%m%d_%H%M%S')}.csv"
    return file_name


def save_reviews(all_reviews, file_name):
    # Convert to pandas dataframe
    df = pd.DataFrame(all_reviews)
    
    # The timestamp can be converted to a date time and added as extra columns:
    df['user_comment_ts'] = pd.to_datetime(df['user_comment_last_modified_seconds'],errors='coerce', unit='s')
    df['dev_comment_ts'] = pd.to_datetime(df['dev_comment_last_modified_seconds'], errors='coerce',unit='s')

    # Save to csv
    df.to_csv(file_name, index=False)
    print(f"All reviews flattened and saved to {file_name}")
