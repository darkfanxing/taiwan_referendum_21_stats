import requests
from bs4 import BeautifulSoup

import pandas as pd
import json
import time
import os
from loguru import logger

from typing import Dict


def __load_data_config(file_path: str) -> Dict:
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def __get_data_links(data_config: Dict) -> pd.DataFrame:
    data = []
    for city in data_config["c00"]["next"]:
        city_serial = data_config[city]["serial"]["c"]
        city_name = data_config[city]["zh"]

        data.append(
            {
                "name": city_name,
                "link": f"https://vote.cec.gov.tw/zh-TW/F1/{city_serial}000000000000.html",
            }
        )

        for district in data_config[city]["next"]:
            district_serial = data_config[district]["serial"]["d"]
            district_name = data_config[district]["zh"]

            data.append(
                {
                    "name": f"{city_name}-{district_name}",
                    "link": f"https://vote.cec.gov.tw/zh-TW/F1/{city_serial}00{district_serial:04s}000000.html",
                }
            )

    data = pd.DataFrame(data)
    return data


def __request_data(
    df_links: pd.DataFrame,
    is_force_request: bool = False,
    request_interval: float = 0.2,
) -> None:
    folder_name = "data/htmls/"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    # column names: name, link
    for _, row in df_links.iterrows():
        file_path = f"{folder_name}/{row['name']}.html"
        if not is_force_request and os.path.exists(file_path):
            continue

        response = requests.get(row["link"])
        if response.status_code == 200:
            logger.info(f"Successfully fetched data for {row['name']}")
        else:
            logger.warning(f"Failed to fetch data for {row['name']}")

        with open(file_path, "w", encoding="ISO-8859-1") as f:
            f.write(response.text)

        time.sleep(request_interval)


def get_data() -> pd.DataFrame:
    if os.path.exists("data/overall.feather"):
        data = pd.read_feather("data/overall.feather")
    else:
        data_config = __load_data_config("src/assets/tree-compiled.min.json")
        df_links = __get_data_links(data_config)
        __request_data(df_links, is_force_request=False)

        data = []
        for _, row in df_links.iterrows():
            filename = f"data/htmls/{row['name']}.html"
            if not os.path.exists(filename):
                continue

            with open(filename, "r", encoding="utf-8") as f:
                html_text = f.read()

            soup = BeautifulSoup(html_text, "html.parser")
            info = {}
            for th, td in zip(soup.find_all("th"), soup.find_all("td")):
                info[th.get_text().strip()] = float(
                    td.get_text().strip().replace(",", "")
                )

            info.update({"name": row["name"]})
            data.append(info)

        data = pd.DataFrame(data)
        data["type"] = data["name"].apply(lambda x: "district" if "-" in x else "city")
        data["city"] = data["name"].apply(lambda x: x.split("-")[0])
        data["district"] = data["name"].apply(
            lambda x: x.split("-")[1] if "-" in x else None
        )
        column_refs = {
            "縣市": "city",
            "鄉鎮": "district",
            "投票數": "voted",
            "同意票數": "voted_agree",
            "不同意票數": "voted_disagree",
            "有效票數": "valid",
            "無效票數": "invalid",
            "投票權人總數": "eligible",
        }
        data.rename(
            columns=column_refs,
            inplace=True,
        )
        data = data[["type"] + list(column_refs.values())]
        data.to_feather("data/overall.feather")
    return data


def save_statistics(data: pd.DataFrame) -> pd.DataFrame:
    folder_name = "data/stats"
    data = data.copy()
    data.rename(columns={"city": "縣市", "district": "鄉鎮"}, inplace=True)

    def __voted(data: pd.DataFrame, type_: str, cols: list) -> None:
        tmp_data = data.copy()
        tmp_data["投票率"] = round(tmp_data["voted"] / tmp_data["eligible"] * 100, 1)
        tmp_data[cols + ["投票率"]].to_csv(
            f"{folder_name}/{type_}/voting_rates.csv", index=False
        )

    def __voted_agree(data: pd.DataFrame, type_: str, cols: list) -> None:
        tmp_data = data.copy()
        tmp_data["同意率（基於投票人數）"] = round(
            tmp_data["voted_agree"] / tmp_data["voted"] * 100, 1
        )
        tmp_data[cols + ["同意率（基於投票人數）"]].to_csv(
            f"{folder_name}/{type_}/voting_agree_rates.csv", index=False
        )

    def __voted_disagree(data: pd.DataFrame, type_: str, cols: list) -> None:
        tmp_data = data.copy()
        tmp_data["不同意率（基於投票人數）"] = round(
            tmp_data["voted_disagree"] / tmp_data["voted"] * 100, 1
        )
        tmp_data[cols + ["不同意率（基於投票人數）"]].to_csv(
            f"{folder_name}/{type_}/voting_disagree_rates.csv", index=False
        )

    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    for type_ in ["district", "city"]:
        tmp_data = data.copy()
        tmp_data = tmp_data[tmp_data["type"] == type_]
        cols = ["縣市", "鄉鎮"] if type_ == "district" else ["縣市"]

        if not os.path.exists(f"{folder_name}/{type_}"):
            os.makedirs(f"{folder_name}/{type_}")

        __voted(tmp_data, type_, cols)
        __voted_agree(tmp_data, type_, cols)
        __voted_disagree(tmp_data, type_, cols)
