import os
import json
import time
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET 
from bs4 import BeautifulSoup
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set global environment file
discord = os.getenv('DISCORD_WEBHOOK')

# Basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_xml_url_and_filename():
    "We get the URL and filename of the most recent XML database file posted on grants.gov"
    day_to_try = datetime.today()


    file_found = None
    # while file found is not defined and day to try is no more than 6 days in the past
    while file_found is None and (datetime.today() - day_to_try).days < 6:
        # try to get the file
        try:
            url = 'https://prod-grants-gov-chatbot.s3.amazonaws.com/extracts/GrantsDBExtract{}v2.zip'.format(
                day_to_try.strftime('%Y%m%d'))
            response = requests.get(url, stream=True)

            if response.status_code == 200:
                file_found = url
            else:
                print('File not found')
                day_to_try = day_to_try - timedelta(days=1)
                logging.info('Trying {}'.format(day_to_try.strftime('%Y%m%d')))
                time.sleep(1)
        except Exception as e:
            logging.error(f'Error: {e}')

        filename = 'GrantsDBExtract{}v2.zip'.format(
            day_to_try.strftime('%Y%m%d'))
        
    print('Found database file {}'.format(filename))
        

    return url, filename


url, filename = get_xml_url_and_filename()

# Uncomment this and change the date if you already found the file and don't want to ping database again
#filename = 'GrantsDBExtract20240303v2.zip'


def download_xml_file(url, filename):
    downloads_dir = './downloads'
    full_path = os.path.join(downloads_dir, filename)
    try:
        # ensure the downloads directory exists
        if not os.path.exists(downloads_dir):
            os.makedirs(downloads_dir)

        # remove all previously downloaded zip files in
        for f in os.listdir(downloads_dir):
            if f.endswith("zip"):
                os.remove(os.path.join(downloads_dir, f))

        logging.info('Downloading...')

        # ping the database url
        response = requests.get(url, stream=True)

        # if file url is found
        if response.status_code == 200:
            handle = open(full_path, "wb")
            for chunk in response.iter_content(chunk_size=512):
                    if chunk: # filter out keep alive new chunks
                        handle.write(chunk)
                
            time.sleep(3)
            logging.info('Downloaded {}'.format(filename))
        else:
            logging.error('File not found')
    except Exception as e:
        logging.error(f'Error: {e}')
        
download_xml_file(url, filename)


# ! Unzip and parse the file
def unzip_and_soupify(filename, unzipped_dirname='unzipped'):
    downloads_dir = './downloads'
    full_path = os.path.join(downloads_dir, filename)

    try:
        if not os.path.exists(unzipped_dirname):
            os.makedirs(unzipped_dirname)
        else:
            # Check if the file already exists in the unzipped directory
            existing_files = os.listdir(unzipped_dirname)
            if existing_files:
                expected_unzipped_filename = filename.replace('.zip', '')
                if expected_unzipped_filename in existing_files:
                    logging.info(f"File {expected_unzipped_filename} already exists in {unzipped_dirname}, skipping deletion.")
                    return
                else:
                    logging.info(f"Deleting existing files in {unzipped_dirname}")
                    for f in existing_files:
                        os.remove(os.path.join(unzipped_dirname, f))

        logging.info('Unzipping...')

        # unzip the raw file
        with zipfile.ZipFile(full_path, "r") as z:
            z.extractall(unzipped_dirname)
        
        # get path of file in unzipped folder
        unzipped_filepath = os.path.join(unzipped_dirname, os.listdir(unzipped_dirname)[0])

        # parse as tree and convert to a string
        tree = ET.parse(unzipped_filepath)
        root = tree.getroot()
        doc = str(ET.tostring(root, encoding='unicode', method='xml'))

        # convert to soup
        soup = BeautifulSoup(doc, 'lxml-xml')
        logging.info(f'Unzipped and soupified {filename}')
    except Exception as e:
        logging.error(f'Error: {e}')

    return soup

soup = unzip_and_soupify(filename)


 # ! Populate df with every xml tag
def soup_to_df(soup):
    try: 
        # convert beautifulsoup object from XML into dataframe
        # list of bs4 FOA objects
        s = 'opportunitysynopsisdetail'
        foa_objs = [tag for tag in soup.find_all() if s in tag.name.lower()]

        # create dictionary from each FOA
        data = []
        for foa in foa_objs:
            # Directly create a dictionary from the children tags
            row = {child.name: child.text for child in foa.findChildren()}
            data.append(row)

        # create dataframe from dictionary
        df = pd.DataFrame(data)

        return df
    except Exception as e:
        logging.error(f'Error: {e}')
        return None


#get full dataframe of all FOAs
foa_df = soup_to_df(soup)

#onvert foa_df to csv if you need to see everything pre-cleaning
#foa_df.to_csv('foa_df.csv')

# uncomment this if you're already working with a csv
#foa_df = pd.read_csv('foa_df.csv')


# ! Filter by dates and keywords
def to_date(date_str):
    """Convert date string from database into date object"""
    # ensure date string is a string
    date_str = str(date_str)
    return datetime.strptime(date_str, '%m%d%Y').date()


def is_recent(date, days=60):
    # Check if date occured within n amount of days from today
    return (datetime.today().date() - to_date(date)).days <= days


def is_open(date):
    # Check if FOA is still open (closedate is in the future)
    if type(date) == float:
        return True
    elif type(date) == str:
        return (datetime.today().date() - to_date(date)).days <= 0


def reformat_date(s):
    # Reformat the date string in human lango
    s = str(s)
    try:
        parsed_date = datetime.strptime(s, '%m%d%Y')
        return parsed_date
    except Exception as e:
        logging.error(f'Error: {e}')
        return None


def sort_by_recent_updates(df):
    # Sort by most recent updates
    new_dates = [reformat_date(i) for i in df['LastUpdatedDate']]

    if new_dates[0] == None:
        logging.error('Error: Could not reformat date in sort_by_recent_updates()')
        return None
    
    df.insert(1, 'UpdateDate', new_dates)
    df = df.sort_values(by=['UpdateDate'], ascending=False)
    logging.info('Sorted by most recent updates')
    return df


def filter_by_keywords(df):
    # Filter by keywords and nonkeywords
    try:
        # get keywords to fiolter dataframe
        keywords = list(pd.read_csv('keywords.csv', header=None)[0])
        keywords_str = '|'.join(keywords)
        # get nonkeywords aka words to avoid
        #nonkeywords = list(pd.read_csv('nonkeywords.csv', header=None)[0])
        #nonkeywords_str = '|'.join(nonkeywords)

        # filter by keywords and nonkeywords
        df = df[df['Description'].str.contains(keywords_str, na=False, case=False)]
        #df = df[~df['description'].str.contains(nonkeywords_str, na=False, case=False)]

        logging.info('Filtered by keywords')
        return df
    except Exception as e:
        logging.error(f'Error: {e}')
        return None
    

def filter_by_opportunityID(df):
    # Filter by opportunity number
    try:
        # get opportunity numbers to fiolter dataframe
        opportunity_numbers = list(pd.read_csv('opportunity_numbers.csv', header=None)[0])
        opportunity_numbers_str = '|'.join(opportunity_numbers)

        # filter by opportunity numbers
        df = df[df['OpportunityID'].str.contains(opportunity_numbers_str, na=False, case=False)]

        logging.info('Filtered by opportunity numbers')
        return df
    except Exception as e:
        logging.error(f'Error: {e}')
        return None


# include only recently updated FOAs
df = foa_df[[is_recent(i) for i in foa_df['LastUpdatedDate']]]

# inlcude only FOAs which are not closed
df = df[[is_open(i) for i in df['CloseDate']]]

# sort by newest FOAs at the top
df = sort_by_recent_updates(df)

# filter by keywords
df = filter_by_keywords(df)

# filter by opportunity numbers if you found the ones you want and realized some formatting was messed up
#df = filter_by_opportunityID(df)

# just so you can look through things manually and see what to keep/delete
df.to_csv('cleaned_df.csv')

# uncomment this if you're already working with a csv
#df = pd.read_csv('cleaned_df.csv')


def create_discord_message(filename, df, print_text=True):
    # get database date
    db_date = filename.split('GrantsDBExtract')[1].split('v2')[0]
    db_date = db_date[:4] + '-' + db_date[4:6] + '-' + db_date[6:]

    # create text
    text = 'Showing {} recently updated FOAs from grants.gov, extracted on {}:'.format(len(df), db_date)
    text += '\n======================================='

    base_hyperlink = r'https://www.grants.gov/search-results-detail/'

    # loop over each FOA title and add to text
    for i in range(len(df)):

        hyperlink = base_hyperlink + str(df['OpportunityID'].iloc[i])

        text += '\n{}) Updated: {},  Closes: {}, Title: {}, {} ({}) \n{}'.format(
            i+1,
            df['UpdateDate'].iloc[i],
            reformat_date(df['CloseDate'].iloc[i]),
            df['OpportunityTitle'].iloc[i].upper(),
            df['OpportunityNumber'].iloc[i],
            df['OpportunityID'].iloc[i], 
            hyperlink)

        text += '\n----------------------------------'
    
    text += '\nFOAs filtered by date and keywords'

    if print_text:
        print(text)
    else:
        logging.info('Created text to send to slack')
    return text


# create the text to send to slack
text = create_discord_message(filename, df)


def send_to_discord(text):
    logging.info('Sending to discord...')
    try:
        # send to discord
        webhook_url = discord
        if webhook_url:
            response = requests.post(
                webhook_url,
                data=json.dumps({'content': text}),
                headers={'Content-Type': 'application/json'})
            if response.status_code == 204:
                logging.info('Sent to discord')
            else:
                logging.error('Failed to send to discord')
        else:
            logging.error('No webhook url found')
    except Exception as e:
        logging.error(f'Error: {e}')

send_to_discord(text)
