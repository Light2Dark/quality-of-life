import time

PROD_DATASET_AQ = "prod.hourly_air_quality"
PROD_DATASET_WEATHER = "prod.hourly_weather"

def timeit(func):
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        print(f"Time taken: {end_time - start_time}")
        with open("benchmark.log", "a") as f:
            f.write(f"Time taken: {end_time - start_time}\n")
        return result
    return wrapper