import unittest, json
from pipelines.etl.transform import transform_aq, transform_weather
from datetime import datetime

class TestTransformingDates(unittest.TestCase):
    def test12am(self):
        self.assertEqual(transform_aq.transforming_dates("2021-05-02", "12:00AM"), datetime(2021,5,2,0,0,0))
        
    def test12pm(self):
        self.assertEqual(transform_aq.transforming_dates("2021-05-02", "12:00PM"), datetime(2021,5,1,12,0,0))
        
    def test1pm(self):
        self.assertEqual(transform_aq.transforming_dates("2021-05-02", "1:00PM"), datetime(2021,5,1,13,0,0))
        
    def test11pm(self):
        self.assertEqual(transform_aq.transforming_dates("2021-05-02", "11:00PM"), datetime(2021,5,1,23,0,0))
        
    def test11am(self):
        self.assertEqual(transform_aq.transforming_dates("2021-05-02", "11:00AM"), datetime(2021,5,1,11,0,0))
        
    def test1am(self):
        self.assertEqual(transform_aq.transforming_dates("2021-05-02", "1:00AM"), datetime(2021,5,1,1,0,0))
        

class CompleteTransformTest(unittest.TestCase):
    def test_combined_weather(self):
        with open("tests/combined_weather_data.json", "r") as f:
            data = json.load(f)
            transform_weather.get_weather_df.fn(data, ["WMSA:9:MY", "WMKK:9:MY"])
            
    def test_aq(self):
        with open("tests/aq_stations_data_24h.json", "r") as f:
            data = json.load(f)
            transform_aq.transform_data.fn(data, "2021-05-02")