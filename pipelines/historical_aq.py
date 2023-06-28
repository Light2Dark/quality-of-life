import pandas as pd
from pipelines.etl.load import upload
from main import PREPROCESSED_AQ_DATA_GCS_SAVEPATH
from infra.prefect_infra import GCS_AIR_QUALITY_BUCKET_BLOCK_NAME

df = pd.read_csv("data/apims-2005-2017.csv", dtype=str)
df2 = pd.read_csv("dbt/seeds/city_states.csv")
df3 = pd.read_csv("dbt/seeds/state_locations.csv")

def find_diff_locations():
    df.rename(columns={"Time": "Datetime"}, inplace=True)
    locations = df.columns.tolist()
    locations.remove("Datetime")
    
    cities = df2["City"].tolist()
    
    identifying_locations = df3["Identifying_Location"].tolist()
    
    diffs_cities_locs = [l for l in locations if l not in cities]
    diffs_id_locs = [l for l in locations if l not in identifying_locations]
    
    print(diffs_cities_locs)
    print("------")
    print(diffs_id_locs)
    
def elt_archive(dataset: str):
    df.rename(columns={"Time": "datetime"}, inplace=True)
    locations = df.columns.tolist()
    locations.remove("datetime")
    
    new_df = pd.melt(df, id_vars=["datetime"], value_vars=locations, var_name="location", value_name="value")
    # new_df.to_csv("data/2005-2017-elt.csv", index=False)
    
    # TODO: add 2017 up to a certain point only since we already have that data
    # TODO: remove null rows
    print(new_df.isna().sum())
    new_df.dropna(axis=0, inplace=True)
    print(new_df.isna().sum())
    
    upload.upload_to_gcs.fn(new_df, "2005-2017-elt", PREPROCESSED_AQ_DATA_GCS_SAVEPATH, GCS_AIR_QUALITY_BUCKET_BLOCK_NAME)

    upload.load_to_bq.fn(new_df, dataset)
    
if __name__ == "__main__":
    # find_diff_locations()
    elt_archive(df, "dev.historic_air_quality")
    pass
    
    