import csv

from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import pandas as pd
from tqdm import tqdm


class CollectInfo:
  def __init__(self, url):
    self.url = url

  def parse_data(self):
    try:
      all_catalog = urlopen(self.url)
      soup = BeautifulSoup(all_catalog, 'html.parser')

      # Собираем список всех категорий
      category_elements = soup.find_all('li', class_='sect')
      for category in tqdm(category_elements, total=len(category_elements), desc='Общий прогресс',
                           unit='Категории', leave=True):
        category_link = category.find('a')['href']

        # Список в который будут помещаться все данные по каждой следующей категории
        category_summary = []

        # Собираем продукты
        category_page_first_page = urlopen(f'{self.url[:22]}{category_link}')
        category_soup_first_page = BeautifulSoup(category_page_first_page, 'html.parser')
        #Записали категорию продукта
        product_category = category_soup_first_page.find('h1').text

        # Находим номер последней страницы в категории
        pages = category_soup_first_page.find('span', class_='nums')
        if pages is not None:
          list_pages = pages.find_all('a')
          num_last_page = int(list_pages[-1].text)
        else:
          num_last_page = 1

        # Если в категории несколько страниц, то проходимся по каждой странице.
        for page in tqdm(range(1, num_last_page+1), total=num_last_page+1, desc='Пагинация', unit='Страницы',
                         leave=True):
          category_page = urlopen(f'{self.url[:22]}{category_link}?PAGEN_1={page}')
          category_soup = BeautifulSoup(category_page, 'html.parser')
          products = category_soup.find_all('div', class_='desc_name')
          del products[::2]

          # А теперь заходим в каждый продукт
          for product in products:
            product_page = urlopen(product.find('a')['href'])
            product_soup = BeautifulSoup(product_page, 'html.parser')

            # Указываем категорию для каждого продукта
            product_cat = product_category

            # Находим артикул
            div_with_art = product_soup.find('div', class_='article iblock')
            if div_with_art is not None:
              product_art = div_with_art.find('span', class_='value').text
            else:
              product_art = 'Не указан'

            # Находим бренд
            product_brand = product_soup.find('a', class_='brand_picture')
            if product_brand is not None:
              product_brand = product_brand.find('img')['title']
            else:
              product_brand = 'Не указан'


            # Находим название
            product_name = product_soup.find('h1')
            if product_name is not None:
              product_name = product_name.text
            else:
              product_art = 'Без названия'

            # Находим цену и убираем все лишнее, оставляя только число
            product_price = product_soup.find('div', class_='price')
            if product_price is not None:
              product_price = re.sub(r'\s|руб\.|\\n', '', product_price.text)
            else:
              product_price = 'Нет в наличии'

            # Находим описание
            product_description = product_soup.find('div', class_='preview_text')
            if product_description is not None:
              product_description = product_description.text
            else:
              product_description = 'Нет описания'

            # Находим изображение
            product_image = []
            image_block = product_soup.find('div', class_='slides').find_all('img')
            if image_block is not None:
              for image in image_block:
                product_image.append(f'https://yacht-parts.ru{image['src']}')
            else:
              product_image = 'Нет изображения'

            # Собираем все параметры в один список
            product_summary = [product_cat, product_art, product_brand, product_name, product_price,
                               product_description, product_image]
            category_summary.append(product_summary)

        # Запись всей информации из категории в файл
        for summary in category_summary:
          data = pd.read_excel('output.xlsx')
          new_row = pd.DataFrame([summary], columns=columns)
          data = pd.concat([data, new_row], ignore_index=True)
          data.to_excel('output.xlsx', index=False)

    except Exception as e:
      print(f"Ошибка при парсинге: {e}")



# Создаем excel файл
columns = ['Категория', 'Артикул', 'Бренд', 'Название', 'Цена', 'Описание', 'Изображение']
df = pd.DataFrame(columns=columns)
df.to_excel('output.xlsx', index=False)

parser = CollectInfo('https://yacht-parts.ru/catalog/')
parser.parse_data()
