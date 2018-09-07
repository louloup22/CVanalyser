#!/usr/bin/env python
"""
coding=utf-8
"""

import logging
import os
import pandas as pd
import textract
import warnings
warnings.filterwarnings('ignore')
import json

import lib
import extract_entities
import extract_text
import spacy
import config_cosmos
import config_search
import config_blob
from BlobConnection import BlobManager

import CosmosConnection
import pydocumentdb.document_client as dc
import SearchConnection



def main():
    """
    Main function documentation template
    :return: None
    :rtype: None
    """
    #Connection to blob
    #blob_connection("../Resume")
    #blob_download()
    
    
    logging.getLogger().setLevel(logging.INFO)

    # Extract data from upstream.
    observations = extract()

    # Spacy: Spacy NLP
    nlp = spacy.load('en')

    # Transform data to have appropriate fields
    observations, nlp = transform(observations, nlp)

    # Load data for downstream consumption
    load(observations, nlp)
    load_to_json(observations, nlp)
    
    #Send data to CosmosDB
    send_to_Cosmos()
    
    #Create Azure search Datasource, Index and indexer
    implement_Azure_search()
    
    
#    DB=CosmosConnection.DBManager(config_cosmos.COSMOSDB_HOST, config_cosmos.COSMOSDB_KEY)
#    new_db_id='db_resume'
#    new_col_id='col_resume'
#    DB.create_database(new_db_id)
#    DB.create_collection(new_db_id,new_col_id)
#    #if new_db_id not in DB.list_databases:
#        #DB.create_database(new_db_id)
#    #if new_col_id not in DB.list_collections(new_db_id):
#        #DB.create_collection(new_db_id,new_col_id)
#    DB.create_doc(new_db_id, new_col_id, load_to_json(observations, nlp))
    pass


def text_extract_utf8(f):
    try:
        return str(textract.process(f), 'utf-8')
    except UnicodeDecodeError as e:
        return ''

def extract():
    logging.info('Begin extract')

    # Reference variables
    candidate_file_agg = list()

    # Create list of candidate files
    for root, subdirs, files in os.walk(lib.get_conf('resume_directory')):
        folder_files = map(lambda x: os.path.join(root, x), files)
        #folder_files = files
        candidate_file_agg.extend(folder_files)

    # Convert list to a pandas DataFrame
    observations = pd.DataFrame(data=candidate_file_agg, columns=['file_path'])
    logging.info('Found {} candidate files'.format(len(observations.index)))
    

    # Subset candidate files to supported extensions
    observations['extension'] = observations['file_path'].apply(lambda x: os.path.splitext(x)[1])
    observations = observations[observations['extension'].isin(lib.AVAILABLE_EXTENSIONS)]
    logging.info('Subset candidate files to extensions w/ available parsers. {} files remain'.
                 format(len(observations.index)))

    #get name of CV
    observations['file']= observations['file_path'].apply(lambda x: os.path.split(x)[1])

    
    # Attempt to extract text from files
    observations['text'] = observations['file_path'].apply(text_extract_utf8)
    observations['chunk_text']=observations['file_path'].apply(extract_text.chunk_text)

    # Archive schema and return
    lib.archive_dataset_schemas('extract', locals(), globals())
    logging.info('End extract')
    return observations


def transform(observations, nlp):
    # TODO Docstring
    logging.info('Begin transform')

    # Extract candidate name
    observations['candidate_name'] = observations['text'].apply(lambda x:
                                                                extract_entities.candidate_name_extractor(x, nlp))
    #Extract nationality
    observations['nationality'] = observations['text'].apply(lambda x:
                                                                extract_entities.nationality_extractor(x, nlp))

    # Extract contact fields
    observations['email'] = observations['text'].apply(lambda x: lib.term_match(x, extract_entities.EMAIL_REGEX))
    observations['phone'] = observations['text'].apply(lambda x: lib.term_match(x, extract_entities.PHONE_REGEX))
    observations['birthdate'] = observations['text'].apply(lambda x: lib.birthdate_match(x, extract_entities.BIRTHDATE_REGEX))
    observations['unit_postcode']= observations['text'].apply(lambda x: lib.term_match(x, extract_entities.UNIT_POSTCODE_REGEX))
    observations['url']= observations['text'].apply(lambda x: lib.term_match(x, extract_entities.URL_REGEX))
    
    # Extract skills
    observations = extract_entities.extract_fields(observations)

    # Archive schema and return
    lib.archive_dataset_schemas('transform', locals(), globals())
    logging.info('End transform')
    return observations, nlp


def load(observations, nlp):
    logging.info('Begin load')
    output_path = os.path.join(lib.get_conf('summary_output_directory'), 'resume_summary.csv')

    logging.info('Results being output to {}'.format(output_path))
    print('Results output to {}'.format(output_path))

    observations.to_csv(path_or_buf=output_path, index_label='index', encoding='utf-8', sep=";")
    logging.info('End transform')
    pass

def load_to_json(observations, nlp):
    logging.info('Begin load to json')
    output_path = os.path.join(lib.get_conf('summary_output_directory'), 'resume_summary.json')

    logging.info('Results being output to {}'.format(output_path))
    print('Results output to {}'.format(output_path))
    
    observations.to_json(path_or_buf=output_path,orient='table')
    logging.info('End transform to json')
    pass

#Put the resume to upload in the Resume directory at first
#def blob_connection(input_path_dir):
#    blob = BlobManager(config_blob.BLOB_NAME,config_blob.BLOB_KEY)
#    blob.create_container(config_blob.BLOB_CONTAINER)
#    #input_path_dir=
#    for filename in os.listdir(input_path_dir):
#        if filename.endswith('.docx'):
#            print(os.path.join(input_path_dir,filename))
#            blob.upload(input_path_dir,filename,config_blob.BLOB_CONTAINER)
##Check that it has worked by downloading all the blob
#    
#def blob_download():
#    blob=BlobManager(config_blob.BLOB_NAME,config_blob.BLOB_KEY)
#    path_where_to_download="../"
#    foldername="Resumefromblob"
#    blob.download_all_blobs(path_where_to_download,foldername,config_blob.BLOB_CONTAINER)

def send_to_Cosmos():
    CosmosConnection.delete_if_exists()
    CosmosConnection.create_db_and_collection()
    input_path=os.path.join(lib.get_conf('summary_output_directory'), 'resume_summary.json')
    client = dc.DocumentClient(config_cosmos.COSMOSDB_HOST, {'masterKey': config_cosmos.COSMOSDB_KEY})
    with open(input_path) as read_file:
        data = json.load(read_file)
    for i in data["data"]:
        client.CreateDocument(CosmosConnection.get_collection_link(config_cosmos.COSMOSDB_DATABASE, config_cosmos.COSMOSDB_COLLECTION), i)
        
#TO DO add delete if exists and check status of index and datasource before doing the search
def implement_Azure_search():
    SearchConnection.delete_datasource(config_search.DATASOURCE_NAME)
    SearchConnection.create_datasource(config_search.DATASOURCE_NAME, config_cosmos.CONNECTION_STRING_SEARCH, config_cosmos.COSMOSDB_COLLECTION, None)
    #SearchConnection.get_datasource(config_search.DATASOURCE_NAME)
    SearchConnection.delete_index(config_search.INDEX_NAME)
    SearchConnection.create_index(config_search.INDEX_NAME)
    SearchConnection.create_indexer(config_search.INDEXER_NAME, config_search.DATASOURCE_NAME, config_search.INDEX_NAME)
    SearchConnection.run_indexer(config_search.INDEXER_NAME)
    SearchConnection.search_docs(config_search.INDEX_NAME, "Louise2")


# Main section
if __name__ == '__main__':
    main()
