import glob
import json
import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import googlemaps
from mojimoji import zen_to_han
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
from gcp_class import Gcs_client

MAPS_API_KEY = "AIzaSyDYQ7VjgrfBPWT9_02Qe1zXhI1cRJvcYOQ"


partition_list = [
    "hittakuri",
    "zitensyatou",
    "syajyounerai",
    "buhinnerai",
    "zihankinerai",
    "zidousyatou",
    "ootobaitou",
]


def geocode(address):
    try:
        gmaps = googlemaps.Client(key=MAPS_API_KEY)
        result = gmaps.geocode(address)
        lat = result[0]["geometry"]["location"]["lat"]
        lng = result[0]["geometry"]["location"]["lng"]

        return lat, lng
    except:
        return None, None


URL = "http://www.geocoding.jp/api/"


def coordinate(address):
    """
    addressに住所を指定すると緯度経度を返す。

    >>> coordinate('東京都文京区本郷7-3-1')
    ['35.712056', '139.762775']
    """
    try:
        payload = {"q": address}
        html = requests.get(URL, params=payload)
        soup = BeautifulSoup(html.content, "html.parser")
        lat = soup.find("lat").string
        lng = soup.find("lng").string
        return lat, lng
    except:
        return None, None


class Gcs_client:
    def __init__(self) -> None:
        # key_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test-gcs.json')
        key_path = "./credential.json"
        service_account_info = json.load(open(key_path))
        self.credentials = service_account.Credentials.from_service_account_info(
            service_account_info
        )
        self.client = storage.Client(
            credentials=self.credentials,
            project=self.credentials.project_id,
        )

    def create_bucket(self, bucket_name):
        """GCSにバケットがなければ作成する。

        Args:
            bucket_name (_type_): _description_
        """

        if self.client.bucket(bucket_name).exists():
            print(f"already exists {bucket_name}")
        else:
            print(f"create {bucket_name}")
            self.client.create_bucket(bucket_name)

    def list_all_objects(self, bucket_name):
        """バケットの中身をリストで出力する。

        Args:
            bucket_name (_type_): _description_

        Returns:
            _type_: _description_
        """
        blobs = self.client.list_blobs(bucket_name)
        all_objects = [blob.name for blob in blobs]
        return all_objects

    def upload_gcs(self, bucket_name, from_path, to_path, dry_run=False):
        """GSCにファイルをアップロードする。

        Args:
            bucket_name (_type_): _description_
            from_path (_type_): _description_
            to_path (_type_): _description_
            dry_run (bool, optional): _description_. Defaults to False.
        """
        print(f"{from_path} to {bucket_name}/{to_path}")
        if dry_run:
            pass
        else:
            bucket = self.client.get_bucket(bucket_name)
            blob_gcs = bucket.blob(to_path)
            # ローカルのファイルパスを指定
            blob_gcs.upload_from_filename(from_path)


class Bigquery_cliant:
    def __init__(self) -> None:
        # key_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test-gcs.json')
        key_path = "./credential.json"
        service_account_info = json.load(open(key_path))
        self.credentials = service_account.Credentials.from_service_account_info(
            service_account_info
        )
        self.client = bigquery.Client(
            credentials=self.credentials,
            project=self.credentials.project_id,
        )

    def read_sql(self, query):
        df = self.client.query(query).to_dataframe()
        return df


def load_data(bq, partition):
    sql = f"""
    SELECT address
    FROM `crimes-porttal.portal_dataset.crimes` 
    WHERE teguchi_en = '{partition}'
    """
    df = bq.read_sql(sql)
    df = df[df["address"] != ""]
    df["address"] = [zen_to_han(a, kana=False) for a in df["address"]]
    return df.reset_index(drop=True)


def main():
    bq = Bigquery_cliant()
    gcs_client = Gcs_client()
    for partition in partition_list:
        df = load_data(bq, partition)
        with open("dic_geo_master.json", "r") as f:
            geo_master = json.load(f)
        i = 0
        for a in tqdm(set(df["address"])):
            print(a)
            if a not in geo_master:
                geo_master[a] = coordinate(a)
                time.sleep(8)
                # geo_master[a] = geocode(a)
                # time.sleep(0.5)
                i += 1
                if i % 10 == 0:
                    with open("dic_geo_master.json", "w") as f:
                        json.dump(geo_master, f)
                    with open("dic_geo_master.json", "r") as f:
                        geo_master = json.load(f)
                    # アップロード
                    clean_geo_master = {
                        add: geo
                        for add, geo in geo_master.items()
                        if (geo != ["0", "0"]) & (geo != [None, None])
                    }
                    df_geo_master = pd.DataFrame(
                        {
                            "address": clean_geo_master.keys(),
                            "cood": clean_geo_master.values(),
                        }
                    )
                    df_geo_master["lat"] = [float(l[0]) for l in df_geo_master["cood"]]
                    df_geo_master["lng"] = [float(l[1]) for l in df_geo_master["cood"]]
                    df_geo_master = df_geo_master.drop("cood", axis=1)
                    local_file_name = f"geo_master.parquet"
                    local_path = f"./output/{local_file_name}"
                    upload_path = f"{local_file_name}"
                    table = pa.Table.from_pandas(df_geo_master, preserve_index=False)
                    pq.write_table(table, local_path)
                    gcs_client.upload_gcs("geo_master", local_path, upload_path)
        with open("dic_geo_master.json", "w") as f:
            json.dump(geo_master, f)


if __name__ == "__main__":
    main()
