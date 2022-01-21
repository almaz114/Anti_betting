from bs4 import BeautifulSoup as bs
import requests
from loguru import logger
from datetime import datetime
import calendar
import pandas as pd
import json
import asyncio
import os
import random
import re
# import csv
import jsonpath as js_path
from jsonpath_ng import jsonpath, parse
from jsonpath_ng.ext import parser   # new method for parsed
from postgresql_main import postgresql_import_csv     # our function import_csv_database


# -- > ЛОГИРОВАНИЕ ОШИБОК/НУЖНОЙ ИНФОРМАЦИИ в определенные файлы < --
logger.add("files//logs//errors.log", format="{time} {level} {message}", rotation="10:00", retention="2 days",
           level="ERROR", compression="zip", enqueue=True)
# logger.add("files//logs//info_debug.log", format="{time} {level} {message}", rotation="10:00", retention="2 days",
# level="INFO", compression="zip", enqueue=True)
# logger.add("files//logs//info_warning.log", format="{time} {level} {message}", rotation="10:00", retention="5 days",
#            level="WARNING", compression="zip", enqueue=True)

# open config files for site/urls
if not os.path.exists("files//config.json"):
    logger.error(f"файл не сушествует по указанному пути files/kovach_signals.json")
else:
    with open('files//config.json') as file:
        data_config = json.load(file)
        urls_bettings = list(data_config.keys())
        # logger.info(f"{urls_bettings=}, {type(data_config)}")

        # configs for postgre_sql
        posgresql_config = data_config['postgresql_config']
        list_csv_files = posgresql_config['list_csv_files']
        database = posgresql_config['database']
        user_name_db = posgresql_config['user_name_db']
        password = posgresql_config['password']
        host = posgresql_config['host']
        port = posgresql_config['port']
        logger.info(f"{posgresql_config=}")

        # configs for schedule
        schedule_confs = data_config['schedule']


# @logger.catch
async def parsing_bet(config_file: dict):
    """ Парсинг сайтов/букмекеров
    1) данные сайтов их urls-адреса берутся из конфиг/json файла
    2) далее в зависимости от сайта применяется, тот или иной метод парсинга
    3) далее собранные сохраняются в json затем конвертирутся в отдельный csv_file (один сайт - один файл)
    4) далее csv_file import to table of Postgre_Sql (один сайт - одна таблица в БД) """

    print(chr(27) + "[2J", end="", flush=True)  # clear console window
    print(chr(27) + "[H", end="", flush=True)

    list_betting = list(config_file.keys())
    time_update = datetime.today().strftime("%d-%m-%Y-%H:%M")

    # запуск цикла парсинга для каждого букмекера
    for i, value in enumerate(list_betting):
        logger.info(f"{value=}")

        # 1. ||| П А Р С И Н Г  Д Л Я  С А Й Т А / Б У К М Е К Е Р А  --> "FONBET"
        if value == "fonbet":
            logger.info(f"fonbet find")
            url = config_file[value][1]     # нужно помнить что для этого url справдливы эти params CURL !!!
            logger.info(f"{url=}")

            headers = {
                'Connection': 'keep-alive',
                'sec-ch-ua': '"Chromium";v="96", "Opera GX";v="82", ";Not A Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 OPR/82.0.4227.50',
                'sec-ch-ua-platform': '"Windows"',
                'Accept': '*/*',
                'Origin': 'https://www.fonbet.ru',
                'Sec-Fetch-Site': 'cross-site',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
                'Referer': 'https://www.fonbet.ru/',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            }

            params = (
                ('place', 'line'),
                ('sysId', '1'),
                ('lang', 'ru'),
                ('salt', 'qxmhqnv7v3ky778sa2'),
                ('supertop', '4'),
                ('scopeMarket', '1600'),
            )

            # response = requests.get('https://line110.bkfon-resources.com/line/desktop/topEvents3', headers=headers, params=params)

            # данная короткая CURL ссылка получена из сайта
            url_config = "https://line110.bkfon-resources.com/line/desktop/topEvents3"

            # 1. Parse Data
            def parse_data_from_url(url: str, headers, params):
                """парсим данные по его url / params / headers  """
                response = requests.get(url_config, timeout=30, headers=headers, params=params)
                champs = url.split('/')[-2].split('-')[0]     # разбиваем строку
                # time_update = datetime.today().strftime("%d-%m-%Y-%H:%M")
                result = response.json()
                # logger.info(f"{type(result)=}, {type(result['events']), {len(result['events'])}=}")

                if isinstance(result['events'], list) and len(result['events']) > 0:
                    return result['events'], champs  # GET BASE_DATA/List_of_dicts (словарь с событиями)  FROM JSON(SERVER)
                else:
                    logger.error(f"данные с сервера либо пустые/либо это список!")
                    result['events'], champs = None, None
                    return result['events'], champs

            # 2. ITERATE FOR DICT FROM ULR/BET_SITE

            def iterate_dicts_url(name_list_url: list, destination_csv_file: str):
                """итерация по списку -словарей из сайта и формирование базового словаря для дальнейшего импорта в csv_file """
                random_value = random.randint(301, 600)       # random для столбца 'id'
                # time_update = datetime.today().strftime("%d-%m-%Y-%H:%M")
                base_dict = {'id': [], 'full_url': [], 'type_stake': [], 'type_sport': [], 'name_liga': [], 'player_1': [], 'player_2': [], 'P1': [], 'X': [], 'P2': [], 'time_update': []}

                date_time = datetime.utcnow()
                UTC = calendar.timegm(date_time.utctimetuple())
                # logger.info(int(UTC))

                for i, value in enumerate(name_list_url):
                    if 'team1' in value and 'team2' in value:
                        logger.info(f"\n{value['id']}=, {value['id']=}")
                        # logger.info(f"{i=}, {len(value['markets'])=}")
                        new_dict = {}
                        # 1. if 'markets':1
                        if len(value['markets']) in (1, 2):
                            if value['markets'][0]['caption'] in ("Исходы", "Основное время"):
                                # logger.warning(f"{len(value['markets'][0]['rows'])=}, {value['markets'][0]['rows']=}")
                                # add new values
                                base_dict['id'].append(UTC + random_value + i)
                                base_dict['full_url'].append(url)
                                base_dict['type_stake'].append(value['place'])
                                base_dict['type_sport'].append(value['skName'])
                                base_dict['name_liga'].append(value['competitionName'])
                                base_dict['player_1'].append(value['team1'])
                                base_dict['player_2'].append(value['team2'])
                                base_dict['time_update'].append(time_update)

                                for j, val in enumerate(value['markets'][0]['rows']):
                                    if 'cells' in val and isinstance(val['cells'], list) and len(val['cells']) in (3, 4):
                                        a_temp = 999   # для определения что начался другой массив
                                        for k, vals in enumerate(val['cells']):
                                            # ||| 1.1 find 'Матч' and his values / aka koef
                                            if 'caption' in vals and vals['caption'] in ('Матч', "Основное время", "Исходы"):
                                                new_dict['caption'] = []
                                                a_temp = j
                                                # logger.info(f"{k=}, {j=}")
                                            if 'value' in vals and isinstance(vals['value'], (int, float)) and 'caption' in new_dict and len(new_dict['caption']) < 3 and a_temp == j:
                                                new_dict['caption'].append(vals['value'])

                                # ||| 1.2 save koef to base_dict
                                if 'caption' in new_dict and len(new_dict['caption']) in (2, 3):
                                    logger.info(f"{new_dict=}, {type(new_dict)=}, {len(new_dict['caption'])=}, almaz_find")
                                if 'caption' in new_dict:
                                    if isinstance(new_dict['caption'], list) and len(new_dict['caption']) in (2, 3):
                                        # logger.info(f"{new_dict=}")
                                        base_dict['P1'].append(new_dict['caption'][0])
                                        if len(new_dict['caption']) == 2:     # когда есть тока 2 коэф-та
                                            base_dict['X'].append(0)
                                            base_dict['P2'].append(new_dict['caption'][1])
                                        elif len(new_dict['caption']) == 3:   # когда есть ничья
                                            base_dict['X'].append(new_dict['caption'][1])
                                            base_dict['P2'].append(new_dict['caption'][2])

                logger.debug(f"\n{len(base_dict['id'])=}, {len(base_dict['P1'])=}")

                # 2. --- S A V E  D I C T  T O  C S V _F I L E
                df_new = pd.DataFrame(base_dict)
                df_new.to_csv(destination_csv_file, index=False, sep='|', date_format='%Y%m%d%H', na_rep="None", encoding='utf-8')

            # START OUR FUNCTIONS
            result_values, champs = parse_data_from_url(url=url_config, headers=headers, params=params)
            iterate_dicts_url(name_list_url=result_values, destination_csv_file="files//result_table_3.csv")

        # 2. ||| П А Р С И Н Г  Д Л Я  С А Й Т А / Б У К М Е К Е Р А  --> "MELBET"
        elif value == "melbet":
            logger.info(f"melbet find")
            # url = config_file[value][0]   # [0] - номер в скобках / line ставки
            url = config_file[value][1]     # нужно помнить что для этого url справдливы эти params CURL !!!
            logger.info(f"{url=}")

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
                'Referer': 'https://melbet.ru/line/tennis/',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            }

            params = (
                ('sports', '4'),
                ('count', '50'),
                ('tf', '1000000'),
                ('mode', '4'),
                ('cyberFlag', '4'),
                ('partner', '195'),
            )

            params_2 = (
                ('sports', '10'),
                ('count', '50'),
                ('tf', '1000000'),
                ('mode', '4'),
                ('cyberFlag', '4'),
                ('partner', '195'),
            )
            await asyncio.sleep(2)
            # данная короткая CURL ссылка получена из сайта
            url_config = "https://melbet.ru/LineFeed/Get1x2_VZip"
            url_config_2 = "https://melbet.ru/LineFeed/Get1x2_VZip"

            # Parse beting site

            def parse_data_from_url(url: str, headers, params):
                """парсим данные по его url / params / headers  """
                response = requests.get(url_config, timeout=30, headers=headers, params=params)
                champs = url.split('/')[-2].split('-')[0]     # разбиваем строку
                # time_update = datetime.today().strftime("%d-%m-%Y-%H:%M")
                result = response.json()
                # logger.info(f"{result=}")
                return result['Value'], champs  # GET BASE_DATA/List_of_dicts (словарь с событиями)  FROM JSON(SERVER)

            # ITERATE FOR DICT FROM ULR/BET_SITE
            def iterate_dicts_url(name_list_url: list, destination_csv_file: str):
                """итерация по списку -словарей из сайта и формирование базового словаря для дальнейшего импорта в csv_file """
                random_value = random.randint(0, 300)       # random для столбца 'id'
                date_time = datetime.utcnow()
                UTC = calendar.timegm(date_time.utctimetuple())
                # time_update = datetime.today().strftime("%d-%m-%Y-%H:%M")
                base_dict = {'id': [], 'full_url': [], 'type_stake': [], 'type_sport': [], 'name_liga': [], 'player_1': [], 'player_2': [], 'P1': [], 'X': [], 'P2': [], 'time_update': []}

                for i, value in enumerate(name_list_url):
                    # logger.info(f"{type(name_list_url)=}, {name_list_url=}")
                    # logger.info(f"\nour {i=}, {value['O1E']=}\n")

                    # проверка наличия второго игрока/ второй строки типа Player 1 vs Player 2
                    # есть ли данный ключ в словаре value, и что есть коэфициент типа float
                    # logger.info(f"{value['E'][:2][0]['C']=}, {type(value['E'][:2][0]['C'])}")
                    if 'O2E' in value and isinstance(value['E'][:2][0]['C'], (float, int)) and isinstance(value['E'][:2][1]['C'], (float, int)):
                        # logger.info(f"key found, its 'okey ")
                        base_dict['id'].append(UTC + random_value + i)
                        base_dict['full_url'].append(url)
                        base_dict['type_stake'].append(champs)
                        base_dict['type_sport'].append(value['SE'])
                        base_dict['name_liga'].append(value['LE'])
                        base_dict['player_1'].append(value['O1'])
                        base_dict['player_2'].append(value['O2'])
                        base_dict['time_update'].append(time_update)

                        # new experiment
                        # jsonpath_result = js_path.jsonpath(value, '$..E[?(@.C && !@.P)]')
                        jsonpath_result = js_path.jsonpath(value, '$..E[?(@.C && !@.P && !@.CE)]')
                        if jsonpath_result is False:
                            logger.error(f"на строке {i=} проблемы, нет данных ")

                        if isinstance(jsonpath_result, list) and len(jsonpath_result) == 2:
                            base_dict['P1'].append(jsonpath_result[0]['C'])
                            base_dict['X'].append(0)
                            base_dict['P2'].append(jsonpath_result[1]['C'])
                        elif isinstance(jsonpath_result, list) and len(jsonpath_result) == 3:
                            base_dict['P1'].append(jsonpath_result[0]['C'])
                            base_dict['X'].append(jsonpath_result[1]['C'])
                            base_dict['P2'].append(jsonpath_result[2]['C'])
                        elif isinstance(jsonpath_result, list) and len(jsonpath_result) > 3:
                            base_dict['P1'].append(jsonpath_result[0]['C'])
                            base_dict['X'].append(jsonpath_result[1]['C'])
                            base_dict['P2'].append(jsonpath_result[2]['C'])
                        else:
                            logger.info(f"not found values, fill zerro:{(jsonpath_result)=}, {type(jsonpath_result)=} ")
                            base_dict['P1'].append(0)
                            base_dict['X'].append(0)
                            base_dict['P2'].append(0)

                    else:
                        logger.warning(f"key not found, second position Player 2 not found !")   # отсутсвие ключа в словаре
                    logger.info(f"{base_dict}\n")
                    # logger.info(f"{base_dict['P1']=}, {base_dict['X']=}, {base_dict['P2']}")
                # logger.info(f"\n{base_dict=}")

                # 2. --- S A V E  D I C T  T O  C S V _F I L E
                df_new = pd.DataFrame(base_dict)
                df_new.to_csv(destination_csv_file, index=False, sep='|', date_format='%Y%m%d%H', na_rep="None", encoding='utf-8')

            # START OUR FUNCTIONS
            result_values, champs = parse_data_from_url(url=url_config, headers=headers, params=params)
            iterate_dicts_url(name_list_url=result_values, destination_csv_file="files//result_table_1.csv")
            await asyncio.sleep(5)

            result_values_2, champs_2 = parse_data_from_url(url=url_config_2, headers=headers, params=params_2)
            iterate_dicts_url(name_list_url=result_values_2, destination_csv_file="files//result_table_2.csv")

        # 3. ||| П А Р С И Н Г  Д Л Я  С А Й Т А / Б У К М Е К Е Р А  --> "1XSTAVKA"
        elif value == "1XSTAVKA".lower():
            logger.info(f"1XSTAVKA find")
            url = config_file[value][0]     # нужно помнить что для этого url справдливы эти params CURL !!!
            logger.info(f"{url=}")

            headers = {
                'Connection': 'keep-alive',
                'sec-ch-ua': '"Chromium";v="96", "Opera GX";v="82", ";Not A Brand";v="99"',
                'Accept': '*/*',
                'X-Requested-With': 'XMLHttpRequest',
                'sec-ch-ua-mobile': '?0',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 OPR/82.0.4227.50',
                'sec-ch-ua-platform': '"Windows"',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
                'Referer': 'https://1xstavka.ru/line/',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            }

            params = (
                ('count', '50'),
                ('tf', '2200000'),
                ('tz', '3'),
                ('antisports', '188'),
                ('mode', '4'),
                ('country', '1'),
                ('partner', '51'),
            )
            await asyncio.sleep(2)
            # response = requests.get('https://1xstavka.ru/LineFeed/Get1x2_VZip', headers=headers, params=params)
            url_config = ""
            url_config = 'https://1xstavka.ru/LineFeed/Get1x2_VZip'

            def parse_data_from_url(url: str, headers, params):
                """парсим данные по его url / params / headers  """
                response = requests.get(url_config, timeout=30, headers=headers, params=params)
                champs = url.split('/')[-2].split('-')[0]     # разбиваем строку
                # time_update = datetime.today().strftime("%d-%m-%Y-%H:%M")
                result = response.json()
                # logger.info(f"{result=}")
                return result['Value'], champs  # GET BASE_DATA/List_of_dicts (словарь с событиями)  FROM JSON(SERVER)

            def iterate_dicts_url(name_list_url: list, destination_csv_file: str):
                """итерация по списку -словарей из сайта и формирование базового словаря для дальнейшего импорта в csv_file """
                random_value = random.randint(900, 1200)       # random для столбца 'id'
                date_time = datetime.utcnow()
                UTC = calendar.timegm(date_time.utctimetuple())
                # time_update = datetime.today().strftime("%d-%m-%Y-%H:%M")
                base_dict = {'id': [], 'full_url': [], 'type_stake': [], 'type_sport': [], 'name_liga': [], 'player_1': [], 'player_2': [], 'P1': [], 'X': [], 'P2': [], 'time_update': []}

                for i, value in enumerate(name_list_url):
                    # проверка наличия второго игрока/ второй строки типа Player 1 vs Player 2
                    # есть ли данный ключ в словаре value, и что есть коэфициент типа float
                    # logger.info(f"{value['E'][:2][0]['C']=}, {type(value['E'][:2][0]['C'])}")
                    if 'O2E' in value and isinstance(value['E'][:2][0]['C'], (float, int)) and isinstance(value['E'][:2][1]['C'], (float, int)):
                        # logger.info(f"key found, its 'okey ")
                        base_dict['id'].append(UTC + random_value + i)
                        base_dict['full_url'].append(url)
                        base_dict['type_stake'].append(champs)
                        base_dict['type_sport'].append(value['SE'])
                        base_dict['name_liga'].append(value['LE'])
                        base_dict['player_1'].append(value['O1'])
                        base_dict['player_2'].append(value['O2'])
                        base_dict['time_update'].append(time_update)

                        # new experiment
                        jsonpath_result = js_path.jsonpath(value, '$..E[?(@.C && !@.P && !@.CE)]')
                        if jsonpath_result is False:
                            logger.error(f"на строке {i=} проблемы, нет данных ")
                        # logger.info(f"{jsonpath_result=}")
                        if isinstance(jsonpath_result, list) and len(jsonpath_result) == 2:
                            base_dict['P1'].append(jsonpath_result[0]['C'])
                            base_dict['X'].append(0)
                            base_dict['P2'].append(jsonpath_result[1]['C'])
                        elif isinstance(jsonpath_result, list) and len(jsonpath_result) == 3:
                            base_dict['P1'].append(jsonpath_result[0]['C'])
                            base_dict['X'].append(jsonpath_result[1]['C'])
                            base_dict['P2'].append(jsonpath_result[2]['C'])
                        elif isinstance(jsonpath_result, list) and len(jsonpath_result) > 3:
                            base_dict['P1'].append(jsonpath_result[0]['C'])
                            base_dict['X'].append(jsonpath_result[1]['C'])
                            base_dict['P2'].append(jsonpath_result[2]['C'])
                        else:
                            logger.info(f"not found values, fill zerro:{(jsonpath_result)=}, {type(jsonpath_result)=} ")
                            base_dict['P1'].append(0)
                            base_dict['X'].append(0)
                            base_dict['P2'].append(0)

                    else:
                        logger.warning(f"key not found, second position Player 2 not found !")   # отсутсвие ключа в словаре
                    # logger.info(f"{base_dict['P1']=}, {base_dict['X']=}, {base_dict['P2']}")
                logger.info(f"\n{len(base_dict['id'])=},\n{len(base_dict['P1'])=}")

                # 2. --- S A V E  D I C T  T O  C S V _F I L E
                df_new = pd.DataFrame(base_dict)
                df_new.to_csv(destination_csv_file, index=False, sep='|', date_format='%Y%m%d%H', na_rep="None", encoding='utf-8')

            # start our functions
            result_values, champs = parse_data_from_url(url=url_config, headers=headers, params=params)
            iterate_dicts_url(name_list_url=result_values, destination_csv_file="files//result_table_4.csv")
            await asyncio.sleep(5)

        # 4. ||| П А Р С И Н Г  Д Л Я  С А Й Т А / Б У К М Е К Е Р А  --> "BETBOOM"
        elif value == "betboom".lower():
            logger.info(f"betboom find")
            url = config_file[value][0]     # нужно помнить что для этого url справдливы эти params CURL !!!
            logger.info(f"{url=}")

            headers = {
                'authority': 'sport.betboom.ru',
                'sec-ch-ua': '"Chromium";v="96", "Opera GX";v="82", ";Not A Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 OPR/82.0.4227.50',
                'sec-ch-ua-platform': '"Windows"',
                'content-type': 'application/x-www-form-urlencoded',
                'accept': '*/*',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-mode': 'cors',
                'sec-fetch-dest': 'empty',
                'referer': 'https://sport.betboom.ru/SportsBook/Upcoming/1546/?game=Australian-Open-%D0%A5%D0%B0%D1%80%D0%B4&gameId=1546',
                'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'cookie': '__cfruid=d9d6d57cacea871474a278820d18dde517746ceb-1642665462; _ym_uid=16426654631006423342; _ym_d=1642665463; _ga=GA1.2.1697111310.1642665463; _gid=GA1.2.825576910.1642665463; _ym_isad=1; supportOnlineTalkID=T1zGiZETmkIvlGaTXR7yyWfpU1wF6Br9; ASP.NET_SesssionId=ulr2r22tycyv14tdf4xcsvbz; __cf_bm=gkEdPsZnueluQHt_RkTJeXG4_0Q143VnD5gyD0UBuJU-1642674519-0-AQZWGGxsq2RFNpOug/oAqamGYzl2JtZM9PSwNFLg+uSA1Rued4pejbZ49HsA26yZejR/YAspJ0wKSaw68c9m3VQ=; _ym_visorc=b',
            }

            params = (
                ('champId', '1546'),
                ('timeFilter', '0'),
                ('langId', '1'),
                ('partnerId', '147'),
                ('countryCode', 'RU'),
            )

            headers_2 = {
                'authority': 'sport.betboom.ru',
                'sec-ch-ua': '"Chromium";v="96", "Opera GX";v="82", ";Not A Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 OPR/82.0.4227.50',
                'sec-ch-ua-platform': '"Windows"',
                'content-type': 'application/x-www-form-urlencoded',
                'accept': '*/*',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-mode': 'cors',
                'sec-fetch-dest': 'empty',
                'referer': 'https://sport.betboom.ru/SportsBook/Upcoming/19879/?game=%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D1%8F-%D0%9B%D0%B8%D0%B3%D0%B0-%D0%9F%D1%80%D0%BE&gameId=19879',
                'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'cookie': '__cfruid=d9d6d57cacea871474a278820d18dde517746ceb-1642665462; _ym_uid=16426654631006423342; _ym_d=1642665463; _ga=GA1.2.1697111310.1642665463; _gid=GA1.2.825576910.1642665463; _ym_isad=1; supportOnlineTalkID=T1zGiZETmkIvlGaTXR7yyWfpU1wF6Br9; ASP.NET_SesssionId=ulr2r22tycyv14tdf4xcsvbz; _ym_visorc=w; __cf_bm=ru4xcXtDqgMsD2nBOT1X_aCFv8gIBFQNZBvKfGJciBc-1642694359-0-ASqIrwETJ3+kLGPW6gHKjlfeaD2IAAnPAUidQ0jxK+s9HuucGa7Q3QR/StD4Fom8oSKIoFJT+qS0eV5KQdAv3YY=; ADRUM_BTa=R:150|g:1f641ac2-ed75-4547-9b38-34ebba560137|n:digitain_7657c2c3-1616-415f-9dbc-71f7586db924; SameSite=None; ADRUM_BT1=R:150|i:129528|e:5',
            }

            params_2 = (
                ('champId', '19879'),
                ('timeFilter', '0'),
                ('langId', '1'),
                ('partnerId', '147'),
                ('countryCode', 'RU'),
            )

            # response = requests.get('https://sport.betboom.ru/Prematch/GetEventsList', headers=headers, params=params)
            await asyncio.sleep(2)
            # response = requests.get('https://1xstavka.ru/LineFeed/Get1x2_VZip', headers=headers, params=params)
            url_config = ""
            url_config = 'https://sport.betboom.ru/Prematch/GetEventsList'
            url_config_2 = "https://sport.betboom.ru/Prematch/GetEventsList"

            def parse_data_from_url(url: str, headers, params):
                """парсим данные по его url / params / headers  """
                response = requests.get(url_config, timeout=30, headers=headers, params=params)
                champs = url.split('/')[-2].split('-')[0]     # разбиваем строку
                # time_update = datetime.today().strftime("%d-%m-%Y-%H:%M")
                result = response.json()
                logger.info(f"{type(result)=}, {result=}")

                return result, champs  # GET BASE_DATA/List_of_dicts (словарь с событиями)  FROM JSON(SERVER)

            def iterate_dicts_url(name_list_url: list, destination_csv_file: str):
                """итерация по списку -словарей из сайта и формирование базового словаря для дальнейшего импорта в csv_file """
                random_value = random.randint(1200, 1300)       # random для столбца 'id'
                date_time = datetime.utcnow()
                UTC = calendar.timegm(date_time.utctimetuple())
                # time_update = datetime.today().strftime("%d-%m-%Y-%H:%M")
                base_dict = {'id': [], 'full_url': [], 'type_stake': [], 'type_sport': [], 'name_liga': [], 'player_1': [], 'player_2': [], 'P1': [], 'X': [], 'P2': [], 'time_update': []}

                for i, value in enumerate(name_list_url):
                    logger.info(f"{i=}; {value['HT']=}")

                    if 'HT' in value and 'AT' in value and isinstance(value['HT'], str) and isinstance(value['AT'], str):
                        base_dict['id'].append(UTC + random_value + i)
                        base_dict['full_url'].append(url)
                        base_dict['type_stake'].append(champs)
                        base_dict['type_sport'].append(value['SN'])
                        base_dict['name_liga'].append(value['CN'])
                        base_dict['player_1'].append(value['HT'])    # name player 1
                        base_dict['player_2'].append(value['AT'])    # name player 2
                        base_dict['time_update'].append(time_update)

                        # if i == 0:
                        jsonpath_result = js_path.jsonpath(value, '$.StakeTypes..Stakes[?(@.GN=="Исход")].[F]')
                        if jsonpath_result is False:
                            logger.error(f"на строке {i=} проблемы, нет данных ")
                        logger.info(f"{type(jsonpath_result)=}, {jsonpath_result=}")

                        if isinstance(jsonpath_result, list) and len(jsonpath_result) == 2:
                            base_dict['P1'].append(jsonpath_result[0])
                            base_dict['X'].append(0)
                            base_dict['P2'].append(jsonpath_result[1])
                        elif isinstance(jsonpath_result, list) and len(jsonpath_result) == 3:
                            base_dict['P1'].append(jsonpath_result[0])
                            base_dict['X'].append(jsonpath_result[1])
                            base_dict['P2'].append(jsonpath_result[2])
                        elif isinstance(jsonpath_result, list) and len(jsonpath_result) > 3:
                            base_dict['P1'].append(jsonpath_result[0])
                            base_dict['X'].append(jsonpath_result[1])
                            base_dict['P2'].append(jsonpath_result[2])
                        else:
                            logger.info(f"not found values, fill zerro:{(jsonpath_result)=}, {type(jsonpath_result)=} ")
                            base_dict['P1'].append(0)
                            base_dict['X'].append(0)
                            base_dict['P2'].append(0)

                    logger.info(f"{base_dict['P1']=}, {base_dict['X']=}, {base_dict['P2']}")
                logger.info(f"\n{len(base_dict['id'])=},\n{len(base_dict['P1'])=}")

                # 2. --- S A V E  D I C T  T O  C S V _F I L E
                df_new = pd.DataFrame(base_dict)
                df_new.to_csv(destination_csv_file, index=False, sep='|', date_format='%Y%m%d%H', na_rep="None", encoding='utf-8')

            # start our functions
            result_values, champs = parse_data_from_url(url=url_config, headers=headers, params=params)
            iterate_dicts_url(name_list_url=result_values, destination_csv_file="files//result_table_5.csv")

            result_values_2, champs_2 = parse_data_from_url(url=url_config_2, headers=headers_2, params=params_2)
            iterate_dicts_url(name_list_url=result_values_2, destination_csv_file="files//result_table_5_1.csv")

            await asyncio.sleep(5)


async def import_csv_postgresql():
    """
    для импорта данных из csv-файлов в таблицу базы данных
    """
    postgresql_import_csv(base_path="files//", list_csv_files=list_csv_files, database=database, user=user_name_db, password=password, port=port, host=host)


# ЗАПУСК ПЛАНИРОВЩИКА ЗАДАЧ ПО РАСПИСАНИЮ
async def checking_task():
    while True:
        if 'seconds' in schedule_confs and schedule_confs['seconds'] != 0:
            await asyncio.sleep(schedule_confs['seconds'])
            await parsing_bet(config_file=data_config)
            await asyncio.sleep(2)
            await import_csv_postgresql()
        elif 'minutes' in schedule_confs and schedule_confs['minutes'] != 0:
            await asyncio.sleep(schedule_confs['minutes'] * 60)
            await parsing_bet(config_file=data_config)
            await asyncio.sleep(2)
            await import_csv_postgresql()
        elif 'hours' in schedule_confs and schedule_confs['hours'] != 0:
            await asyncio.sleep(schedule_confs['hours'] * 3600)
            await parsing_bet(config_file=data_config)
            await asyncio.sleep(2)
            await import_csv_postgresql()

loop = asyncio.get_event_loop()
loop.create_task(checking_task())
loop.run_forever()
