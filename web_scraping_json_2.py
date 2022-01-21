from bs4 import BeautifulSoup as bs
import requests
from loguru import logger
from datetime import datetime
import pandas as pd
import json
import csv


url = 'https://melbet.ru/line/'


headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'sec-ch-ua': '"Chromium";v="96", "Opera GX";v="82", ";Not A Brand";v="99"',
    'Accept': '*/*',
    'X-Requested-With': 'XMLHttpRequest',
    'If-Modified-Since': 'Sat, 1 Jan 2000 00:00:00 GMT',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 OPR/82.0.4227.50',
    'sec-ch-ua-platform': '"Windows"',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://melbet.ru/line/',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
}

params = (
    ('count', '50'),
    ('tf', '1000000'),
    ('mode', '4'),
    ('cyberFlag', '4'),
    ('partner', '195'),
)

time_update = datetime.today().strftime("%d-%m-%Y-%H:%M")


response = requests.get('https://melbet.ru/LineFeed/Get1x2_VZip', headers=headers, params=params)


champs = url.split('/')[-2].split('-')[0]     # разбиваем строку
# logger.info(f"{champs=}")


result = response.json()
# logger.info(f"{result=}")


key_val = result['Value']       # get base datas from json(server)
# logger.info(f"\n\n{key_val}")


# CREATE BASE DICT FROM DATA OF SITE
base_dict = {}
for i, value in enumerate(key_val):
    # logger.info(f" {i=}, {value['CI']=}, \n\n{value['E'][:3]}")
    # j += 1
    inner_dict = {"full_url": '', "type_stake": "", "type_sport": "", "name_liga": "",
                  "player_1": "", "player_2": "", "date_value": "", "P1": "", "X": "", "P2": "", "time_update": ""}

    inner_dict["full_url"] = url
    inner_dict["type_stake"] = champs
    inner_dict["type_sport"] = value['SE']
    inner_dict["name_liga"] = value['LE']
    inner_dict["player_1"] = value['O1E']
    inner_dict["player_2"] = value['O2E']
    inner_dict["P1"] = value['E'][:3][0]['C']
    inner_dict["X"] = value['E'][:3][1]['C']
    inner_dict["P2"] = value['E'][:3][2]['C']
    inner_dict["time_update"] = time_update

    base_dict[str(value['CI'])] = inner_dict

    logger.info(f"{inner_dict=}\n")

    if i == 2:
        break

# 2. second variant create dict
base_dict_2 = {'id': [], 'full_url': [], 'type_stake': [], 'type_sport': [], 'name_liga': [], 'player_1': [],
               'player_2': [], 'P1': [], 'X': [], 'P2': [], 'time_update': []}
# new_list = []

for i, value in enumerate(key_val):
    # inner_dict_2 = {"full_url": '', "type_stake": "", "type_sport": "", "name_liga": "",
    #                 "player_1": "", "player_2": "", "date_value": "", "P1": "", "X": "", "P2": "", "time_update": ""}

    # new_list.append(value['O1E'])
    base_dict_2['id'].append(i)
    base_dict_2['full_url'].append(url)
    base_dict_2['type_stake'].append(champs)
    base_dict_2['type_sport'].append(value['SE'])
    base_dict_2['name_liga'].append(value['LE'])
    base_dict_2['player_1'].append(value['O1E'])
    base_dict_2['player_2'].append(value['O2E'])
    base_dict_2['P1'].append(value['E'][:3][0]['C'])
    base_dict_2['X'].append(value['E'][:3][1]['C'])
    base_dict_2['P2'].append(value['E'][:3][2]['C'])
    base_dict_2['time_update'].append(time_update)

    # base_dict_2[str(value['CI'])] = inner_dict_2
    if i == 2:
        break

logger.info(f"{base_dict_2}")


# Эксперимент
df_new = pd.DataFrame(base_dict_2)
df_new.to_csv("files//result.csv", index=False)


# 3. Third variant create dict for csv file


# SAVE BASE_DICT TO JSON FILE
with open('files/file_1.json', 'w') as file:
    json.dump(base_dict_2, file, ensure_ascii=True, indent=4)
# df = pd.read_json(base_dict)  # read json/dict to dataframe
# logger.info(f"{df=}")

# logger.info(f"\n{base_dict=}")

# OPEN FILE JSON
with open('files/file_1.json', 'r') as file:
        # Чтение файла 'data.json' и преобразование
        # данных JSON в объекты Python
    data = json.load(file)

# С О Х Р А Н И Т Ь  Ф А Й Л .C S V   В  P O S T G R E _S Q L -->
# \copy t from 'd:\path\data.json.csv'

logger.info(f"{base_dict=}")


def convert_dict_to_csv(name_dict: dict):
    """ преобразование словаря в csv формат """
    new_dict = {}
    # logger.info(f"{name_dict.keys()}")

    # 1. create new_dict and fill keys
    for j, val in enumerate(name_dict):
        if j == 0:
            list_keys = name_dict[val].keys()
            # logger.info(f"{list(list_keys)=}")
            for k in list_keys:
                # logger.info(f"{k=}")
                new_dict[k] = []
    logger.info(f"{new_dict=}")

    # 2. insert values to list
    list_1 = []
    for i, value in enumerate(name_dict):
        list_1.append(name_dict[value]['full_url'])
        logger.info(f"{name_dict[value]=}")
    logger.info(f"{list_1=}")


# 4. Save to csv file
header = list(base_dict_2.keys())
data = [
    base_dict_2['id'],
    base_dict_2['full_url'],
    base_dict_2['type_stake'],
    base_dict_2['type_sport'],
    base_dict_2['name_liga'],
    base_dict_2['player_1'],
    base_dict_2['player_2'],
    base_dict_2['P1'],
    base_dict_2['X'],
    base_dict_2['P2'],
    base_dict_2['time_update']
]
with open('files//files_2.csv', 'w', encoding='UTF8', newline='') as f:
    writer = csv.writer(f)
    # write the header
    writer.writerow(header)

    # for i, value in enumerate(base_dict_2):
    #     logger.info(f"{i}")

    # write multiple rows
    writer.writerows(data)


# 5. Save dict to csv
# with open('files//file_3.csv', 'w') as f:
#     for key in base_dict_2.keys():
#         f.write("%s, %s\n" % (key, base_dict_2[key]))

# print(pd.read_csv('files//file_3.csv'))

# Временно
def create_list_values(name_dict: dict):
    """"""
    list_1, list_2 = [], []
    len_a = len(name_dict['id'])
    logger.info(f"{len_a=}")
    for i, value in enumerate(name_dict):
        logger.info(f"{name_dict[value][0]=}, {i=}")
        list_1.append(name_dict[value][0])
        list_2.append(name_dict[value][1])
    logger.info(f"{list_1}\n ,{list_2}")


# create_list_values(base_dict_2)
