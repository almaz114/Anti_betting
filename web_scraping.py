from requests_html import HTMLSession
from bs4 import BeautifulSoup as Soup
from loguru import logger


session = HTMLSession()
url = 'https://melbet.ru/live/football/'
response = session.get(url)
response.html.render(sleep=3, keep_page=True, scrolldown=2)
soup = Soup(
    response.html.raw_html,
    'html.parser'
)
block_id = soup.find_all('div', class_='kofsTableLigaName')

for i, value in enumerate(block_id):
    logger.info(f"{value=}\n")
