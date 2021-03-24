from basic_defs import cloud_storage, NAS
import boto3
import os
import sys

from botocore.exceptions import ClientError

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError

from google.cloud import storage
from google.cloud.exceptions import NotFound

import hashlib

class AWS_S3(cloud_storage):
    def __init__(self):
        # TODO: Fill in the AWS access key ID
        self.access_key_id = ""
        # TODO: Fill in the AWS access secret key
        self.access_secret_key = ""
        # TODO: Fill in the bucket name
        self.bucket_name = ""
        self.s3 = boto3.resource('s3', aws_access_key_id=self.access_key_id,aws_secret_access_key=self.access_secret_key)
        self.bucket = self.s3.Bucket(self.bucket_name)

    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from boto3
    #     boto3.session.Session:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    #     boto3.resources:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    #     boto3.s3.Bucket:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucket
    #     boto3.s3.Object:
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object
    
    def list_blocks(self):
        arr=[]
        for obj in self.bucket.objects.all():
            if obj.key.isdigit():
                arr.append(int(obj.key))
            else:
                arr.append(obj.key)
        return arr
        
    def read_block(self, offset):
        try:
            obj = self.s3.Object(self.bucket_name,str(offset))
            return bytearray(str(obj.get()['Body'].read()))
        except ClientError as e:
            print("AWS cannot read as no resource found")
            return bytearray([])
        
    def write_block(self, block, offset):
        self.bucket.put_object(Body=bytearray(block), Key=str(offset))
        print('Written in AWS')
        
    def delete_block(self, offset):
        obj = self.s3.Object(self.bucket_name,str(offset))
        obj.delete()
        print('AWS deleted at offset')

class Azure_Blob_Storage(cloud_storage):
    def __init__(self):
        # TODO: Fill in the Azure key
        self.key = ""
        # TODO: Fill in the Azure connection string
        self.conn_str = ""
        # TODO: Fill in the account name
        self.account_name = ""
        # TODO: Fill in the container name
        self.container_name = ""
        self.blob_service_client = BlobServiceClient.from_connection_string(self.conn_str)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)


    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from azure.storage.blob
    #    blob.BlobServiceClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python
    #    blob.ContainerClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python
    #    blob.BlobClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python

    def list_blocks(self):
        arr=[]
        try:
            blobs_list = self.container_client.list_blobs()
            for blob in blobs_list:
                if blob.name.isdigit():
                    arr.append(int(blob.name))
                else:
                    arr.append(blob.name)
        except ResourceNotFoundError:
            print("Azure cannot list as no resource found")
        return arr
        
    def read_block(self, offset):
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=str(offset))
            download_stream = blob_client.download_blob()
            stream_data = download_stream.readall()
            return bytearray(stream_data)
        except ResourceNotFoundError:
            print("Azure cannot read as no resource found")
            return bytearray([])
            
        
    def write_block(self, block, offset):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=str(offset))
        blob_client.upload_blob(bytearray(block), blob_type = 'BlockBlob', overwrite = True)
        print('Written in Azure')
        
    def delete_block(self, offset):
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=str(offset))
            blob_client.delete_blob()
            print('Azure deleted at offset')
        except ResourceNotFoundError:
            print("Azure cannot delete as no resource found")

class Google_Cloud_Storage(cloud_storage):
    def __init__(self):
        # Google Cloud Storage is authenticated with a **Service Account**
        # TODO: Download and place the Credential JSON file
        self.credential_file = ""
        # TODO: Fill in the container name
        self.bucket_name = ""
        self.gcp_client = storage.Client.from_service_account_json(self.credential_file)
        self.gcp_bucket = self.gcp_client.get_bucket(self.bucket_name)

    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from google.cloud.storage
    #    storage.client.Client:
    #        https://googleapis.dev/python/storage/latest/client.html
    #    storage.bucket.Bucket:
    #        https://googleapis.dev/python/storage/latest/buckets.html
    #    storage.blob.Blob:
    #        https://googleapis.dev/python/storage/latest/blobs.html
    
    
    def list_blocks(self):
        blobs = list(self.gcp_client.list_blobs(self.gcp_bucket))
        arr = []
        for blob in blobs:
            if blob.name.isdigit():
                arr.append(int(blob.name))
            else:
                arr.append(blob.name)
        return arr
        
    def read_block(self, offset):
        try:
            blob = storage.Blob(str(offset), self.gcp_bucket)
            data = blob.download_as_string()
            return bytearray(data)
        except NotFound:
            print('GCP cannot read as no resource found')
            return bytearray([])
        
    def write_block(self, block, offset):
        blob = storage.Blob(str(offset), self.gcp_bucket)
        blob.upload_from_string(str(block))
        print('Written in GCP')
        
        
        
    def delete_block(self, offset):
        try:
            blob = storage.Blob(str(offset), self.gcp_bucket)
            blob.delete()
            print('GCP deleted at offset')
        except NotFound:
            print('GCP cannot delete as no resource found')

class RAID_on_Cloud(NAS):
    def __init__(self):
        self.backends = [
                AWS_S3(),
                Azure_Blob_Storage(),
                Google_Cloud_Storage()
            ]
        self.block_size = 4096
        self.fd_dictionary = {}
        self.max_fd = 256
        
        
    def getChosenClouds(self, key):
        hash_val = hash(key)
        mod_val = hash_val%3
        chosen_clouds = []
        for i,cloud in enumerate(self.backends):
            if i!=mod_val:
                chosen_clouds.append(cloud)
        sha_hash = hashlib.sha256()
        sha_hash.update(bytearray(key))
        block_offset = sha_hash.hexdigest()
        return chosen_clouds, block_offset

    # Implement the abstract functions from NAS
    def open(self, filename):
        for i in range(self.max_fd):
            #Put the filename in dictionary as a value
            if i not in self.fd_dictionary:
                self.fd_dictionary[i] = filename
                return i

    def read(self, fd, len, offset):
        filename = ""
        if fd in self.fd_dictionary:
            filename = self.fd_dictionary[fd]
        else:
            return bytearray([])
        #Get the starting block number and offset
        first_block = offset//self.block_size
        first_offset = offset%self.block_size
        #Get the ending block number and offset
        last_block = (offset+len-1)//self.block_size
        last_offset = (offset+len-1)%self.block_size
        all_data = ""
        for i in range(first_block, last_block+1):
            key = filename+str(i*4096)
            clouds, block_offset = self.getChosenClouds(key)
            #Get the data from start to end
            data = str(clouds[0].read_block(block_offset))
            if i == first_block:
                start = first_offset 
            else :
                start = 0
            if i == last_block:
                end = last_offset + 1  
            else:
                end = self.block_size
            data = data[start:end]
            all_data += data
        return bytearray(all_data)


    def write(self, fd, data, offset):
        filename = self.fd_dictionary[fd]
        #Block and offset are 0 indexed
        first_block = offset // self.block_size
        first_offset = offset % self.block_size
        last_block = (offset + len(data) - 1) // self.block_size
        last_offset = (offset + len(data) - 1) % self.block_size
        for i in range(first_block, last_block+1):
            key = filename+str(i*4096)
            clouds, block_offset = self.getChosenClouds(key)
            if i == first_block:
                start = first_offset  
            else:
                start = 0
            if i == last_block:
                end = last_offset + 1  
            else:
                end = self.block_size
            data_length = end - start
            block = data[:data_length]
            #Get block data and see if there is already data at the offset
            block_data = str(clouds[0].read_block(block_offset))
            if block_data:
                if i == first_block:
                    first_block_data = block_data[:start]  
                else:
                    first_block_data = ""
                if i == last_block:
                    last_block_data = block_data[end:]  
                else: 
                    last_block_data = ""
                block = first_block_data + block + last_block_data
            for cloud in clouds:
                cloud.write_block(block, block_offset)
            data = data[data_length:]

    def close(self, fd):
        del self.fd_dictionary[fd]

    def delete(self, filename):
        i = 0
        key = filename + str(i * 4096)
        clouds, block_offset = self.getChosenClouds(key)
        data = str(clouds[0].read_block(block_offset))
        status = False if not data else True
        #Delete if data is present
        while status:
            for cloud in clouds:
                cloud.delete_block(block_offset)
            i = i+1
            key = filename + str(i * 4096)
            clouds, block_offset = self.getChosenClouds(key)
            data = str(clouds[0].read_block(block_offset))
            if not data:
                status=False


    

