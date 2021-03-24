import unittest
import os

from basic_defs import cloud_storage
from cloud import AWS_S3, Azure_Blob_Storage, Google_Cloud_Storage
from hexdump import hexdump

class test_cloud_storage(object):
    def __init__(self, cloud_storage):
        self.cloud_storage = cloud_storage

    def test_1_read_write_block(self):
        block1 = bytearray(os.urandom(cloud_storage.block_size))
        self.cloud_storage.write_block(block1, 0)
        self.cloud_storage.write_block(block1, cloud_storage.block_size)
        block2 = self.cloud_storage.read_block(0)
        block3 = self.cloud_storage.read_block(cloud_storage.block_size)
        self.assertTrue(isinstance(block2, bytearray))
        self.assertTrue(isinstance(block3, bytearray))
        self.assertEqual(block1, block2)
        self.assertEqual(block1, block3)
       

    def test_2_list_blocks(self):
        # List all the old blocks
        old_blocks = self.cloud_storage.list_blocks()
        # Find a new block to add
        new_offset = 0
        while new_offset in old_blocks:
            new_offset += cloud_storage.block_size
        # Add the new block
        self.cloud_storage.write_block(bytearray([0] * cloud_storage.block_size), new_offset)
        # List all the blocks
        new_blocks = self.cloud_storage.list_blocks()
        self.assertTrue(len(old_blocks) + 1 == len(new_blocks))
        self.assertTrue(new_offset not in old_blocks)
        self.assertTrue(new_offset in new_blocks)

    def test_3_delete_block(self):
        block1 = bytearray(os.urandom(cloud_storage.block_size))
        self.cloud_storage.write_block(block1, 0)
        self.cloud_storage.write_block(block1, cloud_storage.block_size)
        block2 = self.cloud_storage.read_block(0)
        block3 = self.cloud_storage.read_block(cloud_storage.block_size)
        self.cloud_storage.delete_block(0)
        blocks = self.cloud_storage.list_blocks()
        self.assertFalse(0 in blocks)
        self.assertTrue(cloud_storage.block_size in blocks)
        self.cloud_storage.delete_block(cloud_storage.block_size)
        blocks = self.cloud_storage.list_blocks()
        self.assertFalse(0 in blocks)
        self.assertFalse(cloud_storage.block_size in blocks)

class test_1_AWS_S3(unittest.TestCase, test_cloud_storage):
    def setUp(self):
        test_cloud_storage.__init__(self, AWS_S3())

class test_2_Azure_Blob_Storage(unittest.TestCase, test_cloud_storage):
    def setUp(self):
        test_cloud_storage.__init__(self, Azure_Blob_Storage())

class test_3_Google_Cloud_Storage(unittest.TestCase, test_cloud_storage):
    def setUp(self):
        test_cloud_storage.__init__(self, Google_Cloud_Storage())
        
if __name__ == '__main__':
    unittest.main()
