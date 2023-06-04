import unittest
from datetime import datetime
from pipelines.weather import get_date_chunks

class TestDateChunks(unittest.TestCase):
    def convert_date(self, date_str: str) -> datetime:
        return datetime.strptime(date_str, "%Y%m%d")
    
    def test_less_than_31_days(self):
        self.assertEqual(get_date_chunks(self.convert_date("20210101"), self.convert_date("20210129")), [("20210101", "20210129")])
    
    def test_31_days(self):
        self.assertEqual(get_date_chunks(self.convert_date("20210101"), self.convert_date("20210131")), [("20210101", "20210131")])
    
    def test_more_than_31_days(self):
        self.assertEqual(get_date_chunks(self.convert_date("20210101"), self.convert_date("20210202")), [("20210101", "20210131"), ("20210201", "20210202")])
        
    def test_large_days(self):
        self.assertEqual(get_date_chunks(self.convert_date("20230101"), self.convert_date("20230527")), [("20230101", "20230131"), ("20230201", "20230303"), ("20230304", "20230403"), ("20230404", "20230504"), ("20230505", "20230527")])