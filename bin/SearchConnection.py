#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug  1 11:10:37 2018

@author: LouiseP
"""
#https://docs.microsoft.com/fr-fr/azure/search/search-howto-index-cosmosdb
import json
import requests
import azure.mgmt.search as search
import azure.mgmt.search.models as search_models
import config_search

 
 
#choose name of datasource_name=config_search.DATASOURCE_NAME
#connection_string= config_cosmos.CONNECTION_STRING_SEARCH
#collection_id =config_cosmos.COSMOSDB_COLLECTION
#for the moment collection_query=None
#USE PUT AND NOT POST TO ALLOW UPDATE


class AzureSearchManager(object):
    def __init__(self, api_version, azure_search_name, api_key):
        self.api_version=api_version
        self.azure_search_name=azure_search_name
        self.api_key=api_key
        
    def create_datasource(self,datasource_name, connection_string, collection_id, collection_query):
        url = 'https://' + self.azure_search_name + ".search.windows.net/datasources/"+ datasource_name +"?api-version=" + self.api_version
        headers = {
            'Content-Type': 'application/json',
            'api-key': self.api_key
        }
        data = {
            "name": datasource_name,
            "type": "documentdb",
            "credentials": {
                "connectionString": connection_string
            },
            "container": {"name": collection_id, "query": collection_query},
    #strategy of detection of data changes by identifying modified data
            "dataChangeDetectionPolicy": {
                "@odata.type": "#Microsoft.Azure.Search.HighWaterMarkChangeDetectionPolicy",
                "highWaterMarkColumnName": "_ts" #_ts= propriety timestamp
            },
    #strategy of soft delete by identifying the delete data
            "dataDeletionDetectionPolicy": {
                "@odata.type": "#Microsoft.Azure.Search.SoftDeleteColumnDeletionDetectionPolicy",
                "softDeleteColumnName": "isDeleted",
                "softDeleteMarkerValue": "true"
            }
        }
        response = requests.put(url, headers=headers, data=json.dumps(data))
        print("Created datasource:"+ str(response))
        
        def get_datasource(self,datasource_name):
            url="https://"+ self.azure_search_name +".search.windows.net/datasources/"+ datasource_name +"?api-version="+self.api_version
            headers = {'api-key': self.api_key}
            response=requests.get(url,headers=headers)
            print("Datasource definition is " + str(response))
            
        
        def create_index(self,index_name):
            url = 'https://' + self.azure_search_name + '.search.windows.net/indexes/' + index_name + '?api-version=' + self.api_version
            headers = {
                'Content-Type': 'application/json',
                'api-key': self.api_key
            }
            data = {
             "name": index_name,
             "fields":[
                {"name":"index","type":"Edm.Int64"},
                {"name":"file_path","type":"Edm.String"},
                {"name":"extension","type":"Edm.String"},
                {"name":"file","type":"Edm.String"},
                {"name":"text","type":"Edm.String"},
                {"name":"chunk_text","type":"Edm.String"},
                {"name":"candidate_name","type":"Edm.String"},
                {"name":"nationality","type":"Edm.String"},
                {"name":"email","type":"Edm.String"},
                {"name":"phone","type":"Edm.String"},
                {"name":"birthdate","type":"Edm.String"},
                {"name":"unit_postcode","type":"Edm.String"},
                {"name":"url","type":"Edm.String"},
                {"name":"experience","type":"Edm.String"},
                {"name":"position","type":"Edm.String"},
                {"name":"level_of_education","type":"Edm.String"},
                {"name":"platforms","type":"Edm.String"},
                {"name":"typestudies","type":"Edm.String"},
                {"name":"universities","type":"Edm.String"},
                {"name":"languages","type":"Edm.String"},
                {"name":"languages_accreditation","type":"Edm.String"},
                {"name":"hobbies","type":"Edm.String"},
                {"name":"programming","type":"Edm.String"},
                {"name":"database","type":"Edm.String"},
                {"name":"machinelearning","type":"Edm.String"},
                {"name":"cloud_platform","type":"Edm.String"},
                {"name":"open_source","type":"Edm.String"},
                {"name":"IT_business_toolkit","type":"Edm.String"},
                {"name":"id","type":"Edm.String","key":True,"searchable": False}
                ],
             "suggesters": [
                {
                "name": "education",
                "searchMode": "analyzingInfixMatching",
                "sourceFields": ["universities", "typestudies","level_of_education"]
                }]
            }
            response = requests.put(url, headers=headers, data=json.dumps(data))
            #print(response.status_code)
            #TO PRINT IF PB print(json.loads(response.content))
            print("Created index:" + str(response))
            
        def create_indexer(indexer_name, datasource_name, index_name):
            url = 'https://' + self.azure_search_name + '.search.windows.net/indexers/' + indexer_name + '?api-version=' + self.api_version
            headers = {
                'Content-Type': 'application/json',
                'api-key': self.api_key
            }
            data = {
                "name": indexer_name,
                "dataSourceName": datasource_name,
                "targetIndexName": index_name
            }
            response = requests.put(url, headers=headers, data=json.dumps(data))
            #TO PRINT IF PB print(json.loads(response.content))
            print("Created indexer:" + str(response))
            
        def get_indexer_status(self, indexer_name):
            url = 'https://' + self.azure_search_name + '.search.windows.net/indexers/' + indexer_name + '/status?api-version=' + self.api_version
            headers = {'api-key': self.api_key}
            response = requests.get(url, headers=headers)
            print(json.loads(response.content))
            print("get indexer status: " + str(response))
        
        def run_indexer(self, indexer_name):
            url = 'https://' + self.azure_search_name + '.search.windows.net/indexers/' + indexer_name + '/run?api-version=' + self.api_version
            headers = {'api-key': self.api_key}
            response = requests.post(url, headers=headers)
            print("run indexer: " + str(response))
            
        #def search_docs(index_name, query,fields="*",count=False ):
        def search_docs(self, index_name, query):
            url='https://' + self.azure_search_name + '.search.windows.net/indexes/' + index_name + '/docs/search?api-version=' + self.api_version
            headers = {
                'Content-Type': 'application/json',
                'api-key': self.api_key
            }
            data={
                    #"count":count,
                    "search":query,
                    #"searchFields":fields
                    #"select":"file"
                  }
            response = requests.post(url, headers=headers, data=json.dumps(data))
            print(json.loads(response.content))
            print(response)
            
            
            



def create_datasource(datasource_name, connection_string, collection_id, collection_query):
    url = 'https://' + config_search.SEARCH_NAME + ".search.windows.net/datasources/"+ datasource_name +"?api-version=" + config_search.API_VERSION
    headers = {
        'Content-Type': 'application/json',
        'api-key': config_search.API_KEY
    }
    data = {
        "name": datasource_name,
        "type": "documentdb",
        "credentials": {
            "connectionString": connection_string
        },
        "container": {"name": collection_id, "query": collection_query},
#strategy of detection of data changes by identifying modified data
        "dataChangeDetectionPolicy": {
            "@odata.type": "#Microsoft.Azure.Search.HighWaterMarkChangeDetectionPolicy",
            "highWaterMarkColumnName": "_ts" #_ts= propriety timestamp
        },
#strategy of soft delete by identifying the delete data
        "dataDeletionDetectionPolicy": {
            "@odata.type": "#Microsoft.Azure.Search.SoftDeleteColumnDeletionDetectionPolicy",
            "softDeleteColumnName": "isDeleted",
            "softDeleteMarkerValue": "true"
        }
    }
    response = requests.put(url, headers=headers, data=json.dumps(data))
    print("Created datasource:"+ str(response))

def delete_datasource(datasource_name):
    url='https://' + config_search.SEARCH_NAME + ".search.windows.net/datasources/"+ datasource_name +"?api-version=" + config_search.API_VERSION
    headers = {
        'api-key': config_search.API_KEY
    }
    response = requests.delete(url, headers=headers)
    print("Deleted datasource:"+ str(response))

def get_datasource(datasource_name):
    url="https://"+ config_search.SEARCH_NAME +".search.windows.net/datasources/"+ datasource_name +"?api-version="+config_search.API_VERSION
    headers = {'api-key': config_search.API_KEY}
    response=requests.get(url,headers=headers)
    print("Datasource definition is " + str(response))

##def update_datasource(datasource_name):
##    url="https://"+config_search.SEARCH_NAME+".search.windows.net/datasources/"+ datasource_name +"?api-version="+config_search.API_VERSION
##    headers = {'api-key': config_search.API_KEY}
  #  response=requests.put(url,headers=headers)
   # print("Datasource definition is " + str(response))
   
   
def delete_index(index_name):
    url = 'https://' + config_search.SEARCH_NAME + '.search.windows.net/indexes/' + index_name + '?api-version=' + config_search.API_VERSION
    headers = {
        'api-key': config_search.API_KEY
    }
    response = requests.delete(url, headers=headers)
    print("Deleted index:"+ str(response))

def create_index(index_name):
    url = 'https://' + config_search.SEARCH_NAME + '.search.windows.net/indexes/' + index_name + '?api-version=' + config_search.API_VERSION
    headers = {
        'Content-Type': 'application/json',
        'api-key': config_search.API_KEY
    }
    data = {
     "name": index_name,
     "fields":[
        {"name":"index","type":"Edm.Int64"},
        {"name":"file_path","type":"Edm.String"},
        {"name":"extension","type":"Edm.String"},
        {"name":"file","type":"Edm.String"},
        {"name":"text","type":"Edm.String"},
        {"name":"chunk_text","type":"Edm.String"},
        {"name":"candidate_name","type":"Edm.String"},
        {"name":"nationality","type":"Edm.String"},
        {"name":"email","type":"Edm.String"},
        {"name":"phone","type":"Edm.String"},
        {"name":"birthdate","type":"Edm.String"},
        {"name":"unit_postcode","type":"Edm.String"},
        {"name":"url","type":"Edm.String"},
        {"name":"experience","type":"Edm.String"},
        {"name":"position","type":"Edm.String"},
        {"name":"level_of_education","type":"Edm.String"},
        {"name":"platforms","type":"Edm.String"},
        {"name":"typestudies","type":"Edm.String"},
        {"name":"universities","type":"Edm.String"},
        {"name":"languages","type":"Edm.String"},
        {"name":"languages_accreditation","type":"Edm.String"},
        {"name":"hobbies","type":"Edm.String"},
        {"name":"programming","type":"Edm.String"},
        {"name":"database","type":"Edm.String"},
        {"name":"machinelearning","type":"Edm.String"},
        {"name":"cloud_platform","type":"Edm.String"},
        {"name":"open_source","type":"Edm.String"},
        {"name":"IT_business_toolkit","type":"Edm.String"},
        {"name":"id","type":"Edm.String","key":True,"searchable": False}
        ],
     "suggesters": [
        {
        "name": "education",
        "searchMode": "analyzingInfixMatching",
        "sourceFields": ["universities", "typestudies","level_of_education"]
        }]
    }
    response = requests.put(url, headers=headers, data=json.dumps(data))
    #print(response.status_code)
    #TO PRINT IF PB print(json.loads(response.content))
    print("Created index:" + str(response))

#indexer_name=config_search.INDEXER_NAME
#datasource_name=config_search.DATASOURCE_NAME
#index_name=config_search.INDEX_NAME
def create_indexer(indexer_name, datasource_name, index_name):
    url = 'https://' + config_search.SEARCH_NAME + '.search.windows.net/indexers/' + indexer_name + '?api-version=' + config_search.API_VERSION
    headers = {
        'Content-Type': 'application/json',
        'api-key': config_search.API_KEY
    }
    data = {
        "name": indexer_name,
        "dataSourceName": datasource_name,
        "targetIndexName": index_name
    }
    response = requests.put(url, headers=headers, data=json.dumps(data))
    #TO PRINT IF PB print(json.loads(response.content))
    print("Created indexer:" + str(response))

def get_indexer_status(indexer_name):
    url = 'https://' + config_search.SEARCH_NAME + '.search.windows.net/indexers/' + indexer_name + '/status?api-version=' + config_search.API_VERSION
    headers = {'api-key': config_search.API_KEY}
    response = requests.get(url, headers=headers)
    print(json.loads(response.content))
    print("get indexer status: " + str(response))

def run_indexer(indexer_name):
    url = 'https://' + config_search.SEARCH_NAME + '.search.windows.net/indexers/' + indexer_name + '/run?api-version=' + config_search.API_VERSION
    headers = {'api-key': config_search.API_KEY}
    response = requests.post(url, headers=headers)
    print("run indexer: " + str(response))
    
#def search_docs(index_name, query,fields="*",count=False ):
def search_docs(index_name, query):
    url='https://' + config_search.SEARCH_NAME + '.search.windows.net/indexes/' + index_name + '/docs/search?api-version=' + config_search.API_VERSION
    headers = {
        'Content-Type': 'application/json',
        'api-key': config_search.API_KEY
    }
    data={
            #"count":count,
            "search":query,
            #"searchFields":fields
            #"select":"file"
          }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    print(json.loads(response.content))
    print(response)



#get_indexer_status(config_search.INDEXER_NAME)
#run_indexer(config_search.INDEXER_NAME)    
search_docs(config_search.INDEX_NAME, "Louise")

       