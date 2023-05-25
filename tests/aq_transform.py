import unittest
from pipelines.etl.transform.transform_aq import transforming_dates
from datetime import datetime

class TestTransformingDates(unittest.TestCase):
    def test12am(self):
        self.assertEqual(transforming_dates("2021-05-02", "12:00AM"), datetime(2021,5,2,0,0,0))
        
    def test12pm(self):
        self.assertEqual(transforming_dates("2021-05-02", "12:00PM"), datetime(2021,5,1,12,0,0))
        
    def test1pm(self):
        self.assertEqual(transforming_dates("2021-05-02", "1:00PM"), datetime(2021,5,1,13,0,0))
        
    def test11pm(self):
        self.assertEqual(transforming_dates("2021-05-02", "11:00PM"), datetime(2021,5,1,23,0,0))
        
    def test11am(self):
        self.assertEqual(transforming_dates("2021-05-02", "11:00AM"), datetime(2021,5,1,11,0,0))
        
    def test1am(self):
        self.assertEqual(transforming_dates("2021-05-02", "1:00AM"), datetime(2021,5,1,1,0,0))