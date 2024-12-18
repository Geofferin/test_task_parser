import asyncio
import aiohttp
from bs4 import BeautifulSoup
import csv
import re
from tqdm import tqdm


class CollectInfo:
    def __init__(self, url):
        self.url = url


    async def creation_async_task(self, func, category_name, session, headers, des_list):
        # Проходимся по переданным урлам и все данные с каждой страницы записываем в переменную tasks
        tasks = []
        for i in des_list:
            task = asyncio.create_task(func(i, category_name, session, headers))
            tasks.append(task)
        return await asyncio.gather(*tasks) # Запускаем асинхронное выполнение задачи


    async def parse_product(self, product_url, product_cat, session, headers):
        async with session.get(url=product_url, headers=headers) as response:
            product_soup = BeautifulSoup(await response.text(), 'lxml')

            try:
                # Указываем категорию для каждого продукта
                product_cat = product_cat

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
                    product_name = 'Без названия'

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

                # Находим изображения
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
            except AttributeError:
                # Первый элемент понятен, затем указываем еще 7 элементов с помощью генератора
                product_summary = ['Страница товара не найдена']+(['-'] * 7)

            return product_summary


    async def parse_category(self, url, category_name, session, headers):
        async with session.get(url=url, headers=headers) as response:
            soup = BeautifulSoup(await response.text(), "lxml")

            if soup:
                # Находим ссылки на сами товары
                products = soup.find_all('div', class_='desc_name')
                del products[::2]
                product_links = [product.find('a')['href'] for product in products]

                result = await self.creation_async_task(self.parse_product, category_name, session, headers, product_links)

                return result


    async def parse_pagination(self, category_url, category_name, session, headers):
        async with session.get(url=category_url, headers=headers) as response:
            soup = BeautifulSoup(await response.text(), "lxml")

            if soup:
                # Находим количество страниц в каждой категории
                pages = soup.find('span', class_='nums')
                if pages is not None:
                    list_pages = pages.find_all('a')
                    num_last_page = int(list_pages[-1].text)
                else:
                    num_last_page = 1

                # Создаем список адресов для перехода мо этим страницам
                pag_urls = [f'{category_url}?PAGEN_1={page}' for page in range(num_last_page + 1)]

                try:
                    # Если страниц слишком много, то разбиваем на несколько подсписков и проходимся по каждому в отдельности
                    results = []
                    if len(pag_urls) > 40:
                        pag_urls = [pag_urls[i:i + 40] for i in range(0, len(pag_urls), 40)]
                        for page in pag_urls:
                            # Вызываем функцию, которая асинхронно запускает следующие нужные функции
                            results.append(await self.creation_async_task(self.parse_category, category_name, session, headers, page))
                    else:
                        results.append(await self.creation_async_task(self.parse_category, category_name, session, headers, pag_urls))
                except TimeoutError:
                    results = None

                """
                По какой то причине, В ПЕРВОМ ЭЛЕМЕНТЕ (РЕЗУЛЬТАТЕ) всегда дублируется первый элемент:
                    то есть, всегда дублируется самая первая страница с первыми 20 товарами.
                Решил эту проблему тупейшим костылем. В дальнейшем нужно исправить дублирование первой страницы.
                """
                if results:
                    chunk = results[0]
                    del chunk[1]

                    # Записываем данные в csv файл
                    with open('async_output.csv', 'a', newline='', encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile)
                        for result in results:
                            for r in result:
                                writer.writerows(r)
                else:
                    print(f'Не удалось получить категорию: {category_name}')

    async def parse_data(self):
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36"
        }

        # Создаем файл, куда будут записываться все данные
        with open('async_output.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Категория', 'Артикул', 'Бренд', 'Название', 'Цена', 'Описание', 'Изображение'])

        async with aiohttp.ClientSession() as session:
            main_page = await session.get(self.url, headers=headers)
            soup = BeautifulSoup(await main_page.text(), 'html.parser')
            if soup:
                category_elements = soup.find_all('li', class_='sect')
                for category in tqdm(category_elements, total=len(category_elements), desc='Общий прогресс',
                                     unit='Категории', leave=True):
                    category_url = self.url[:22] + category.find('a')['href']
                    await self.parse_pagination(category_url, category.text, session, headers)



parser = CollectInfo('https://yacht-parts.ru/catalog/')
asyncio.run(parser.parse_data())
