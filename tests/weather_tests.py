import unittest
from datetime import datetime
from pipelines.weather import get_date_chunks

class TestDateChunks(unittest.TestCase):
    def convert_date(self, date_str: str) -> datetime:
        return datetime.strptime(date_str, "%Y%m%d")
    
    def test_less_than_31_days(self):
        self.assertEqual(get_date_chunks(self.convert_date("20210101"), self.convert_date("20210129")), [("20210101", "20210129")])
    
    def test_31_days(self):
        self.assertEqual(get_date_chunks(self.convert_date("20210101"), self.convert_date("20210201")), [("20210101", "20210201")])
    
    def test_more_than_31_days(self):
        self.assertEqual(get_date_chunks(self.convert_date("20210101"), self.convert_date("20210202")), [("20210101", "20210201"), ("20210202", "20210202")])
        
    def test_large_days(self):
        self.assertEqual(get_date_chunks(self.convert_date("20230101"), self.convert_date("20230527")), [("20230101", "20230201"), ("20230202", "20230305"), ("20230306", "20230406"), ("20230407", "20230508"), ("20230509", "20230527")])