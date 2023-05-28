import unittest
from pipelines.weather import get_date_chunks

class TestDateChunks(unittest.TestCase):
    def test_less_than_31_days(self):
        self.assertEqual(get_date_chunks("20210101", "20210129"), [("20210101", "20210129")])
    
    def test_31_days(self):
        self.assertEqual(get_date_chunks("20210101", "20210201"), [("20210101", "20210201")])
    
    def test_more_than_31_days(self):
        self.assertEqual(get_date_chunks("20210101", "20210202"), [("20210101", "20210201"), ("20210202", "20210202")])
        
    def test_large_days(self):
        self.assertEqual(get_date_chunks("20230101", "20230527"), [("20230101", "20230201"), ("20230202", "20230305"), ("20230306", "20230406"), ("20230407", "20230508"), ("20230509", "20230527")])