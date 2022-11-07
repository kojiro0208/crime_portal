import os
import json
import glob
import re
import pandas as pd
from mojimoji import zen_to_han
from google.cloud import bigquery
import pyarrow as pa
import pyarrow.parquet as pq

from gcp_class import Gcs_client, Bigquery_cliant

cols_dict = {
    "罪名": "zaimei",
    "手口": "teguchi",
    "管轄警察署": "keisatsusyo",
    "管轄交番・駐在所": "kouban",
    "都道府県": "prefecture",
    "市区町村コード": "city_code",
    "市区町村": "city",
    "町丁目": "cyoume",
    "address": "address",
    "nendo": "nendo",
    "発生年月日": "occurrence_day",
    "発生時": "occurrence_time",
    "発生場所": "occurrence_point",
    "発生場所の詳細": "occurrence_point_info",
    "被害者の性別": "victim_sex",
    "被害者の年齢": "victim_age",
    "被害者の職業": "victim_job",
    "現金被害の有無": "is_financial_damage",
    "施錠関係": "sejyou",
    "現金以外の主な被害品": "other_damage",
    "盗難防止装置の有無": "is_device",
    "file_name": "file_name",
}

ja_to_en = {
    "ひったくり": "hittakuri",
    "自転車盗": "zitensyatou",
    "車上ねらい": "syajyounerai",
    "部品ねらい": "buhinnerai",
    "自動販売機ねらい": "zihankinerai",
    "自動車盗": "zidousyatou",
    "オートバイ盗": "ootobaitou",
}


def add_all_cols(df, cols_list):
    """データフレームにないカラムに欠損を加える。

    Args:
        df (_type_): _description_
        cols_list (_type_): _description_

    Returns:
        _type_: _description_
    """
    no_cols = [c for c in cols_list if c not in df.columns]
    for c in no_cols:
        df[c] = float("nan")
    return df[cols_list]


def read_csv_all_encode(file):
    """各種encodeのcsvを読み込む

    Args:
        file (_type_): _description_
    """
    try:
        d = pd.read_csv(file, encoding="shift-jis")
    except:
        try:
            d = pd.read_csv(file, encoding="cp932")
        except:
            d = pd.read_csv(file)
    return d


# GCSにアップロード
BUCKET_NAME = "crime_dashboard"
gcs_client = Gcs_client()
gcs_client.create_bucket(BUCKET_NAME)
files = glob.glob("./data/**/*.csv", recursive=True)
fields = []
for c in cols_dict.values():
    fields.append(pa.field(c, pa.string()))
table_schema = pa.schema(fields)
all_objects = gcs_client.list_all_objects(BUCKET_NAME)
for f in files:
    d = read_csv_all_encode(f)

    d["file_name"] = os.path.basename(f)
    d["nendo"] = int(re.findall("20\d\d", f)[0])
    d.columns = [
        c.replace("（発生地）", "").replace("（始期）", "").replace("\n", "") for c in d.columns
    ]
    d.columns = [
        zen_to_han(c, kana=False).replace('"', "").replace("[", "") for c in d.columns
    ]
    for c in d.select_dtypes("object").columns:
        d[c] = d[c].str.replace('"|”', "")
    d = d.rename(columns={"発生場所の属性": "発生場所の詳細"})
    d["address"] = d["都道府県"] + d["市区町村"].fillna("") + d["町丁目"].fillna("")
    d["address"] = [zen_to_han(a, kana=False) for a in d["address"]]
    d = d[~d["罪名"].isna()]
    if len(d) == 0:
        continue
    # 列名を英字に変換
    d.columns = [cols_dict.get(c) for c in d.columns]
    d = add_all_cols(d, list(cols_dict.values()))
    # 欠損をから文字で埋める
    d = d.fillna("")
    # 全てを文字列に
    for c in d.columns:
        d[c] = d[c].astype(str)
    # ファイル名に件名、フォルダ名に手口をつける。
    pref = os.path.dirname(f).split("/")[-1]
    teguchi = ja_to_en[d["teguchi"].values[0]]
    local_file_name = os.path.basename(f.lower()).replace(".csv", "")
    local_file_name = f"{pref}_{local_file_name}.parquet"
    local_path = f"./output/{local_file_name}"
    upload_path = f"teguchi_en={teguchi}/{local_file_name}"
    # すでにファイルがあればスキップ
    if upload_path in set(all_objects):
        continue
    table = pa.Table.from_pandas(d, schema=table_schema, preserve_index=False)
    pq.write_table(table, local_path)
    gcs_client.upload_gcs(BUCKET_NAME, local_path, upload_path)


# テーブル作成
bq_cliant = Bigquery_cliant()
table_id = "crimes-porttal.portal_dataset.crimes"
url = f"gs://{BUCKET_NAME}/*"
source_uri_prefix = f"gs://{BUCKET_NAME}"

schema = []
for c in cols_dict.values():
    schema.append(bigquery.SchemaField(c, "STRING", mode="NULLABLE"))

bq_cliant.create_external_table(table_id, url, source_uri_prefix, schema)


# マスター
BUCKET_NAME = "geo_master"
gcs_client = Gcs_client()
gcs_client.create_bucket(BUCKET_NAME)
with open("dic_geo_master.json", "r") as f:
    geo_master = json.load(f)

df_geo_master = pd.DataFrame(
    {
        "address": geo_master.keys(),
        "cood": geo_master.values(),
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
gcs_client.upload_gcs(BUCKET_NAME, local_path, upload_path)
