import unittest
import os
import random
import string

from cloud import RAID_on_Cloud

class test_1_RAID_on_Cloud(unittest.TestCase):
    def setUp(self):
        self.NAS = RAID_on_Cloud()

    def test_1_1_open_write_read_small(self, test_size=10):
        filename = ''.join(random.choice(string.ascii_letters) for i in range(32))
        data1 = ''.join(random.choice(string.ascii_letters) for i in range(test_size))
        fd = self.NAS.open(filename)
        self.NAS.write(fd, data1.encode('utf-8'), 0)
        self.NAS.close(fd)
        data2 = self.NAS.read(fd, test_size, 0).decode('utf-8')
        self.assertTrue(len(data2) == 0)
        fd = self.NAS.open(filename)
        data3 = self.NAS.read(fd, test_size, 0).decode('utf-8')
        self.assertEqual(data1, data3)

    def test_1_2_open_write_read_middle(self):
        self.test_1_1_open_write_read_small(1000)

    def test_1_3_open_write_read_large(self):
        self.test_1_1_open_write_read_small(10000)

    def test_2_1_overwrite_small(self, test_size=10):
        filename = ''.join(random.choice(string.ascii_letters) for i in range(32))
        data1 = ''.join(random.choice(string.ascii_letters) for i in range(test_size))
        data2 = ''.join(random.choice(string.ascii_letters) for i in range(test_size))
        fd = self.NAS.open(filename)
        self.NAS.write(fd, data1.encode('utf-8'), 0)
        self.NAS.close(fd)
        fd = self.NAS.open(filename)
        self.NAS.write(fd, data2.encode('utf-8'), 0)
        self.NAS.close(fd)
        fd = self.NAS.open(filename)
        data3 = self.NAS.read(fd, test_size, 0).decode('utf-8')
        self.assertEqual(data2, data3)

    def test_2_2_overwrite_middle(self):
        self.test_2_1_overwrite_small(1000)

    def test_2_3_overwrite_large(self):
        self.test_2_1_overwrite_small(10000)

    def test_3_1_overlapped_write_small(self, test_size=10):
        filename = ''.join(random.choice(string.ascii_letters) for i in range(32))
        data1 = ''.join(random.choice(string.ascii_letters) for i in range(test_size))
        data2 = ''.join(random.choice(string.ascii_letters) for i in range(test_size))
        fd = self.NAS.open(filename)
        self.NAS.write(fd, data1.encode('utf-8'), 0)
        self.NAS.close(fd)
        fd = self.NAS.open(filename)
        self.NAS.write(fd, data2.encode('utf-8'), test_size / 2)
        self.NAS.close(fd)
        fd = self.NAS.open(filename)
        data3 = self.NAS.read(fd, test_size / 2, 0).decode('utf-8')
        data4 = self.NAS.read(fd, test_size, test_size / 2).decode('utf-8')
        self.assertEqual(data1[:test_size / 2], data3)
        self.assertEqual(data2, data4)

    def test_3_2_overlapped_write_middle(self):
        self.test_3_1_overlapped_write_small(1000)

    def test_3_3_overlapped_write_large(self):
        self.test_3_1_overlapped_write_small(10000)

    def test_4_multiple_write_read(self, test_size=100):
        filename1 = ''.join(random.choice(string.ascii_letters) for i in range(32))
        filename2 = ''.join(random.choice(string.ascii_letters) for i in range(32))
        data1 = ''.join(random.choice(string.ascii_letters) for i in range(test_size))
        data2 = ''.join(random.choice(string.ascii_letters) for i in range(test_size))
        fd1 = self.NAS.open(filename1)
        fd2 = self.NAS.open(filename2)
        self.NAS.write(fd1, data1.encode('utf-8'), 0)
        self.NAS.write(fd2, data2.encode('utf-8'), 0)
        self.NAS.close(fd1)
        self.NAS.close(fd2)
        fd1 = self.NAS.open(filename2)
        fd2 = self.NAS.open(filename1)
        data3 = self.NAS.read(fd1, test_size, 0).decode('utf-8')
        data4 = self.NAS.read(fd2, test_size, 0).decode('utf-8')
        self.NAS.close(fd1)
        self.NAS.close(fd2)
        self.assertEqual(data1, data4)
        self.assertEqual(data2, data3)

    def test_5_delete(self, test_size=100):
        filename = ''.join(random.choice(string.ascii_letters) for i in range(32))
        data1 = ''.join(random.choice(string.ascii_letters) for i in range(test_size))
        fd = self.NAS.open(filename)
        self.NAS.write(fd, data1.encode('utf-8'), 0)
        self.NAS.close(fd)
        self.NAS.delete(filename)
        fd = self.NAS.open(filename)
        data2 = self.NAS.read(fd, test_size, 0).decode('utf-8')
        self.assertTrue(len(data2) == 0)

if __name__ == '__main__':
    unittest.main()
