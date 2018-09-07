#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 27 10:41:33 2018

@author: LouiseP
"""
#Script to extract the CV stored in the blob and store in a data directory
from __future__ import absolute_import
from __future__ import division, print_function, unicode_literals


from azure.storage.blob import BlockBlobService, PublicAccess
import zipfile
import os
import config_blob


#Helper class to deal with Blob
#------account_name=blob_account_name
#------account_key=blob_account_key
class BlobManager(object):
    def __init__(self, account_name,account_key):
        self.block_blob_service=BlockBlobService(account_name=blob_account_name,account_key=blob_account_key)

    def create_container(self, container_name):
        containers = self.block_blob_service.list_containers()
        for container in containers:
            if container.name == container_name:
                return #container already exists
        self.block_blob_service.create_container(container_name)
        #Set permission as public
        self.block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
            
    def upload(self, path, filename, container_name):
        full_file_path=os.path.join(path,filename)
        print(full_file_path)
        self.block_blob_service.create_blob_from_path(container_name, filename, full_file_path)
    
    def create_blob_from_path(self, container_name,blob,full_file_path):
        self.block_blob_service.create_blob_from_path(container_name,blob,full_file_path)
    
    def upload_from_bytes(self, data, filename, container_name):
        self.block_blob_service.create_blob_from_bytes(container_name, filename, data)
    
    def download(self, path, filename, newfoldername, container_name):
        zf=zipfile.ZipFile(os.path.join(path,container_name + '.zip'),
                           mode='w',
                           compression=zipfile.ZIP_DEFLATED)
        b= self.block_blob_service.get_blob_to_bytes(container_name, filename)
        zf.writestr(filename.replace(" ", ""), b.content)
        zf.close()  
        #.replace('.docx', '_Downloaded.docx')
        zip_ref=zipfile.ZipFile(os.path.join(path,container_name + '.zip'),'r')
        zip_ref.extractall(os.path.join(path, newfoldername))
        zip_ref.close()
        
     
        full_file_path=os.path.join(path, filename)
        self.block_blob_service.get_blob_to_path(container_name,filename,full_file_path)
        
    def list_blobs(self, container_name):
        print("\nList blobs in the container")
        generator= self.block_blob_service.list_blobs(container_name)
        for blob in generator:
            print("\t Blob name: " + blob.name.replace(" ", ""))
        return generator
    
    def get_blob_url(self, container_name, blob_name):
        return self.block_blob_service.make_blob_url(container_name, blob_name)
    
    
    def delete_container(self, container_name):
        self.block_blob_service.delete_container(container_name)
        
    def delete_blob(self, container_name, blob_name):
            self.block_blob_service.delete_blob(container_name, blob_name)
    
    def download_all_blobs(self, path, newfoldername, container_name):
        generator=self.block_blob_service.list_blobs(container_name)
        zf=zipfile.ZipFile(os.path.join(path,container_name + '.zip'),
                           mode='w',
                           compression=zipfile.ZIP_DEFLATED)
        for blob in generator:
            b= self.block_blob_service.get_blob_to_bytes(container_name, blob.name)
            zf.writestr(blob.name.replace(" ", ""), b.content)
        zf.close()
        #zip_ref=zipfile.ZipFile(os.path.join(path,container_name + '.zip'),'r')
        #zip_ref.extractall(os.path.join(path, newfoldername))
        #zip_ref.close()
        
        


blob_account_name = config_blob.BLOB_NAME
blob_account_key = config_blob.BLOB_KEY
mycontainer= config_blob.BLOB_CONTAINER

block_blob_service = BlockBlobService(account_name=blob_account_name,account_key=blob_account_key) 

#check of the blobs stored in the container
print("\nList blobs in the container")
generator = block_blob_service.list_blobs(mycontainer)
for blob in generator:
    print("\t Blob name: " + blob.name.replace(" ", ""))


#download all the blobs from the container, and write these blobs in a zip file via get_blob_to_bytes.
#creates a resumeblob.zip
zf = zipfile.ZipFile(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))+ '/data/input/'+mycontainer+'.zip', 
             mode='w',
             compression=zipfile.ZIP_DEFLATED
             )

for blob in generator:
    b = block_blob_service.get_blob_to_bytes(mycontainer, blob.name)
    zf.writestr(blob.name.replace(" ", ""), b.content)
zf.close()


#Unzip zipfile in a new folder Datablob in the parent folder
#there should be an easier method to retrieve the parent folder
zip_ref = zipfile.ZipFile(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))+ '/data/input/'+mycontainer+'.zip', 'r')
zip_ref.extractall(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))+'/data/input/Datablob')
zip_ref.close()

