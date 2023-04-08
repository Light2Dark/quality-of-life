import requests
import time
import threading

def make_100_requests():
    # make 100 requests in 1 minute
    start_time = time.time()

    for i in range(100):
        response = requests.get('http://apims.doe.gov.my/data/public_v2/MCAQM/mcaqmhours24/2018/08/17/0000.json')
        print(response.status_code)

        # sleep for 0.6 seconds to ensure 100 requests in 1 minute
        time.sleep(1.2)

    end_time = time.time()
    print(f'Total time taken: {end_time - start_time} seconds')
    
def fetch(url):
    response = requests.get(url)
    print(response.status_code)
    
def concurrent_request(num_threads):
    url = "http://apims.doe.gov.my/data/public_v2/MCAQM/mcaqmhours24/2018/08/17/0000.json"
    threads = [threading.Thread(target=fetch, args=(url,)) for _ in range(num_threads)]
    
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    
if __name__ == "__main__":
    # make_100_requests()
    concurrent_request(20)
    pass