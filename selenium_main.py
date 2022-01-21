from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys             # упраелние с клавы
from selenium.webdriver.chrome.service import Service
import time
from loguru import logger
import os


options = webdriver.ChromeOptions()
options.add_argument("user_agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/   96.0.4664.110 Safari/537.36 ")
url = "https://www.google.ru/"

s = Service('chrome_driver\\chromedriver.exe')
driver = webdriver.Chrome(service=s)


def clear():
    return os.system('cls')


try:
    driver.get(url=url)

    time.sleep(1)
    logger.info(f"almaz test")
    clear()

    input_field_2 = driver.find_element(By.NAME, "q")           # пример новый find element по 'name'
    # element = driver.find_element(By.ID, "element_id")        # пример новый find element по его 'id'
    # element = driver.find_element(By.LINK_TEXT, "element_link_text")   # пример новый find element по его 'LINK_TEXT'
    # element = driver.find_element(By.TAG_NAME, "element_tag_name")     # пример новый find element по его 'element_tag_name'
    # element = driver.find_element(By.CSS_SELECTOR, "element_css_selector")    # пример новый, по его 'CSS_SELECTOR'
    input_field_2.send_keys(("где мои ботинки чувак ?"))    # в поле поиска вводим слово python
    driver.save_screenshot("test_screenschoot.png")

    time.sleep(15)
    input_field_2.clear()       # очишаем поле / элемент

    input_field_2.send_keys(("а нет давай в бильярд сгоняем ?"))
    time.sleep(5)
    driver.close()      # закроем вкладку
    driver.quit()       # закроем браузер


except Exception as ex:
    logger.info(f"{ex=}")


#  --------------------------------------
# driver = webdriver.Firefox()         # открываем браузер
# driver.get('https://www.google.ru/')  # открываем сайт ко-й вписали


# # assert "Google" in driver.page_source   # проверяем сайт на наличие слова google на главной странице
# # time.sleep(5)
# # driver.quit()    # закрываем браузер


# class GoogleSearch(unittest.TestCase):
#     def setUp(self):
#         self.driver = webdriver.Firefox()
#         self.driver.get('https://www.google.ru/')

#     def test_01(self):
#         driver = self.driver
#         input_field = driver.find_element_by_class_name("gLFyf gsfi")
#         input_field.send_keys(("python"))    # в поле поиска вводим слово python
#         input_field.send_keys(Keys.ENTER)    # и вводим нажатие кнопки Enter

#         time.sleep(3)
