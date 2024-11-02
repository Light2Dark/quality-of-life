import unittest
import pandas as pd

class TestRelationships(unittest.TestCase):
    df_state_locations = pd.read_csv("dbt/seeds/state_locations.csv")
    df_city_places = pd.read_csv("dbt/seeds/city_places.csv")
    df_city_states = pd.read_csv("dbt/seeds/city_states.csv")
    
    def test_unique_location(self):
        self.assertTrue(self.df_state_locations['Identifying_Location'].is_unique, 'Identifying_Location is not unique')
        
    def test_location_rltship_places(self):
        for place in self.df_state_locations['Place']:
            self.assertTrue(place in self.df_city_places['Place'].values, f'Place {place} not in city_places')
            
    def test_city_rltship_state(self):
        for city in self.df_city_places['City']:
            self.assertTrue(city in self.df_city_states['City'].values, f'City {city} not in city_states')
            
    def test_unique_pws_station(self):
        df = self.df_state_locations
        unique_len = len(df[df['PWStation'].duplicated() & df['PWStation'].notna()])
        self.assertTrue(unique_len == 0, 'PWStation is not unique')
        
        
if __name__ == "__main__":
    unittest.main()