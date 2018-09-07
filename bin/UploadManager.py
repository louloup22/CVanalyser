#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 17:22:51 2018

@author: LouiseP
"""

#TO DO : NEEDS to erase all the resumeblob.zip in login_web_app and all the contents of the data/input folder.
#Input folder to create if does not exist.





# This script is to manage the upload of doc in Blob and their automatic adding into CosmosDB
from __future__ import absolute_import
from __future__ import division, print_function, unicode_literals

import config_cosmos
import config_blob
import os
from BlobConnection import BlobManager
import CosmosConnection
import main
import logging
import spacy
import lib
import pydocumentdb.document_client as dc
import json
import sys


def get_name_of_filepath(filepath):
    if filepath.endswith('.docx'):
        return filepath
    else:
        return 'NOT A WORD DOCUMENT'

def blob_connection(data,filename,output_path):
    blob = BlobManager(config_blob.BLOB_NAME,config_blob.BLOB_KEY)
    blob.upload_from_bytes(filename,data,config_blob.BLOB_CONTAINER)
    blob.download(output_path,filename,config_blob.BLOB_CONTAINER)
    
def blob_connection_bytes(filename, data):
    blob = BlobManager(config_blob.BLOB_NAME,config_blob.BLOB_KEY)
    blob.create_blob_from_bytes(config_blob.BLOB_CONTAINER, filename, data)

def blob_connection_path(path,filename,output_path):
    blob = BlobManager(config_blob.BLOB_NAME,config_blob.BLOB_KEY)
    blob.create_blob_from_path(config_blob.BLOB_CONTAINER, filename, os.path.join(path,filename))
    
    #upload(path,filename,config_blob.BLOB_CONTAINER)
    blob.download(output_path,filename,config_blob.BLOB_CONTAINER)

def send_to_Cosmos():
    input_path=os.path.join(lib.get_conf('summary_output_directory'), 'resume_summary.json')
    client = dc.DocumentClient(config_cosmos.COSMOSDB_HOST, {'masterKey': config_cosmos.COSMOSDB_KEY})
    with open(input_path) as read_file:
        data = json.load(read_file)
    for i in data["data"]:
        client.CreateDocument(CosmosConnection.get_collection_link(config_cosmos.COSMOSDB_DATABASE, config_cosmos.COSMOSDB_COLLECTION), i)


def UploadManager_path(filepath, output_directorypath):
    blob = BlobManager(config_blob.BLOB_NAME,config_blob.BLOB_KEY)

    if get_name_of_filepath(filepath)!='NOT A WORD DOCUMENT':
        print(filepath)
        path=os.path.split(get_name_of_filepath(filepath))[0]
        filename=os.path.split(get_name_of_filepath(filepath))[1]
        blob.upload(path,filename,config_blob.BLOB_CONTAINER)
        print('blob connection')
        directory_output=os.path.split(output_directorypath)[1]
        print(directory_output)
        root_output=os.path.split(output_directorypath)[0]
        print(root_output)
        #blob.download(output_directorypath,filename,directory_output, config_blob.BLOB_CONTAINER)
        blob.download_all_blobs(root_output,directory_output,config_blob.BLOB_CONTAINER)
        
        
        logging.getLogger().setLevel(logging.INFO)
        
        # Extract data from upstream.
        observations = main.extract()

        # Spacy: Spacy NLP
        nlp = spacy.load('en')
    
        # Transform data to have appropriate fields
        observations, nlp = main.transform(observations, nlp)
    
        # Load data for downstream consumption
        main.load(observations, nlp)
        main.load_to_json(observations, nlp)
        
        #Send data to CosmosDB
        send_to_Cosmos()
        print('sent to cosmos')
    else: 
        print('error' + filepath)
        return 'NOT A WORD DOCUMENT'



def upload_from_bytes(self, data, filename, container_name):
        self.block_blob_service.create_blob_from_bytes(container_name, filename, data)
#filename=docFile.name
#data=docFile.data
#output_directorypath=docFileRootPath
def UploadManager(filename):
    #blob = BlobManager(config_blob.BLOB_NAME,config_blob.BLOB_KEY)

    if get_name_of_filepath(filename)!='NOT A WORD DOCUMENT':
        print(filename)
        #blob.upload_from_bytes(data,filename,config_blob.BLOB_CONTAINER)
        #print('blob connection')
        #directory_output=os.path.split(output_directorypath)[1]
        #print(directory_output)
        #root_output=os.path.split(output_directorypath)[0]
        #print(root_output)
        #blob.download(output_directorypath,filename,directory_output, config_blob.BLOB_CONTAINER)
        #blob.download_all_blobs(root_output,directory_output,config_blob.BLOB_CONTAINER)
        
        main.blob_download()
        
        logging.getLogger().setLevel(logging.INFO)
        
        # Extract data from upstream.
        observations = main.extract()

        # Spacy: Spacy NLP
        nlp = spacy.load('en')
    
        # Transform data to have appropriate fields
        observations, nlp = main.transform(observations, nlp)
    
        # Load data for downstream consumption
        main.load(observations, nlp)
        main.load_to_json(observations, nlp)
        
        #Send data to CosmosDB
        main.send_to_Cosmos()
        print('sent to cosmos')
        print("Data is ready to be send to cosmos")
        
        #Create Azure search Datasource, Index and indexer
        main.implement_Azure_search()
    else: 
        print('error')
        return 'NOT A WORD DOCUMENT'

#Main execution body
#if __name__ == '__main__':
filedata=sys.argv[2]
print("/n Output from Python    ")
print("filename " + sys.argv[1])
print("filedata " + filedata)
print("outputdir  " + sys.argv[3])

#binary_data = bytes([65, 66, 67])  # ASCII values for A, B, C
#text = binary_data.decode('utf-8')
#print(text)


#print(bytes(filedata,'utf-8'))
filedata=filedata.split(',')
#print(filedata)
buffer=list(map(int,filedata))
print(buffer)
#new_list=[]
#for b in buffer:
#    new_list.append(chr(buffer[b]))
#print('new list is ' + str(new_list))
#i=int.from_bytes(buffer,byteorder='big')
#print(i)
#
binary_data=bytes(buffer)

UploadManager(sys.argv[1])
#print(binary_data)

#text=binary_data.decode('utf-8')
#print(text)
#print("           " + os.path.join(sys.argv[2],sys.argv[1]))
#UploadManager(sys.argv[1],bytearray(filedata,'utf-8'),sys.argv[3])
#UploadManager_path('/Users/jean-marcpicard/Documents/MICROSOFT/Test/CV_Louise2.docx',"../data/input/Datablob")
    

#class UploadManager(object):