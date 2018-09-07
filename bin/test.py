#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 31 16:56:51 2018

@author: LouiseP
"""
import json
import lib
import os
#import CosmosConnection
#import pydocumentdb.document_client as dc
#import config_cosmos
#client = dc.DocumentClient(config_cosmos.COSMOSDB_HOST, {'masterKey': config_cosmos.COSMOSDB_KEY})
#print(client.ReadDocuments(CosmosConnection.create()['_self']))

input_path=os.path.join(lib.get_conf('summary_output_directory'), 'resume_summary.json')
#client = dc.DocumentClient(config_cosmos.COSMOSDB_HOST, {'masterKey': config_cosmos.COSMOSDB_KEY})
with open(input_path) as read_file:
    data = json.load(read_file)
print(len(data["data"]))
#    client.CreateDocument(CosmosConnection.create()['_self'], i)