from bs4 import BeautifulSoup as bs
import requests
from loguru import logger


url = 'https://melbet.ru/live/football/'
url_2 = 'https://melbet.ru/LiveFeed/Get1x2_VZip?champs=88637&count=50&mode=4&cyberFlag=4&partner=195'
url_3 = "https://melbet.ru/line/football/118587-uefa-champions-league/"


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
    'Referer': 'https://melbet.ru/line/football/118587-uefa-champions-league/',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
}


champs = url_3.split('/')[-2].split('-')[0]     # разбиваем строку
logger.info(f"{champs=}")


params = (
    ('champs', '118587'),
    ('count', '50'),
    ('tf', '1000000'),
    ('mode', '4'),
    ('cyberFlag', '4'),
    ('partner', '195'),
)

response = requests.get('https://melbet.ru/LineFeed/Get1x2_VZip', params=params, headers=headers)


result = response.json()
# logger.info(f"{result=}")


key_val = result['Value']
# logger.info(f"\n\n{key_val}")


for i, value in enumerate(key_val):
    logger.info(f"{value['CI']=}, \n\n{value['E'][:3]}")
    base_dict = {value['CI']: {}}
    break
