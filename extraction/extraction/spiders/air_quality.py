import scrapy

class AirQuality(scrapy.Spider):
    name="air_quality" # name of spider
    
    start_urls = [
        'http://apims.doe.gov.my/api_table.html'
    ]

    def parse(self, response):
        filename='air_quality.html'
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log(f'Saved file {filename}')