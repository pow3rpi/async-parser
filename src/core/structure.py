import csv
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional

import asyncio
from aiohttp import ClientSession
from bs4 import BeautifulSoup, ResultSet

from src.core import config
from src.dto.car import Car


class Base:

    @staticmethod
    async def _get_page(url: str, session: ClientSession) -> Optional[BeautifulSoup]:
        html: str

        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            html = await resp.text()

        return BeautifulSoup(html, 'html.parser')


class DromPagination(Base):

    @staticmethod
    def _build_url(page_no: int, brand: str) -> str:
        return f'{config.DOMAIN}{brand}{config.PAGE_PREFIX}{page_no}/'

    @staticmethod
    def _extract_item_links(html: BeautifulSoup) -> Optional[List[str]]:
        items: ResultSet
        items_links: str

        try:
            items = html.select('a.e1huvdhj1')
            item_links = [item.get('href') for item in items]
        except Exception as e:
            print(str(e))
            item_links = None

        return item_links

    @classmethod
    async def _get_item_links_from_page(cls, page_no: int, session: ClientSession, brand: str) -> Optional[List[str]]:
        url: str
        html: Optional[BeautifulSoup]
        item_links: Optional[List[str]]

        url = cls._build_url(page_no, brand)
        html = await cls._get_page(url, session)
        item_links = cls._extract_item_links(html)

        return item_links

    @classmethod
    async def get_item_links(cls, session: ClientSession, brand: str, num_pages: int) -> List[str]:
        tasks: List[Any]
        data: List[List[str]]
        item_links: List[str]

        tasks = [
            cls._get_item_links_from_page(page_no, session, brand)
            for page_no in range(1, num_pages + 1)
        ]
        data = await asyncio.gather(*tasks)

        item_links = []
        for elem in data:
            if elem:
                item_links.extend(elem)

        return item_links

    @classmethod
    async def check_page(cls, page_no: int, session: ClientSession, brand: str) -> bool:
        url: str
        html: Optional[BeautifulSoup]

        url = cls._build_url(page_no, brand)
        html = await cls._get_page(url, session)
        if html.select_one('a.e1huvdhj1'):
            return True

        return False


class DromItem(Base):

    @staticmethod
    def _extract_model(html: BeautifulSoup, brand: str) -> str:
        model: Optional[BeautifulSoup] | str
        ind_space: int
        # block: Optional[BeautifulSoup]

        try:
            model = html.select_one('span.e162wx9x0')
            model = model.text.split(',')[0]
            ind_space = model.find(' ')
            model = model[ind_space + 1:]
            # if you want to use models in Cyrillic use the following
            # block = html.select_one('a.eg1bwqy0')
            # model = block.get('title')
        except Exception as e:
            print(str(e))
            model = brand.title() if brand != 'bmw' else brand.upper()

        return model

    @staticmethod
    def _extract_year(html: BeautifulSoup) -> str:
        year: BeautifulSoup | str

        try:
            year = html.select_one('span.e162wx9x0')
            year = re.findall(r'\d{4}\sгод', year.text)[0]
            year = re.sub(r'\D', '', year)
        except Exception as e:
            print(str(e))
            year = ''

        return year

    @staticmethod
    def _extract_price(html: BeautifulSoup) -> str:
        price: BeautifulSoup | str

        try:
            price = html.select_one('div.e162wx9x0')
            price = re.sub(r'\D', '', price.text)
        except Exception as e:
            print(str(e))
            price = ''

        return price

    @staticmethod
    def _extract_characteristics(html: BeautifulSoup) -> Dict[str, str]:
        description: ResultSet
        characteristics: Dict[str, str]

        try:
            description = html.find_all('tr')
        except Exception as e:
            print(str(e))
            characteristics = defaultdict(lambda: '')
        else:
            characteristics = defaultdict(lambda: '')
            for param in description:
                if param.find('th') and param.find('td'):
                    characteristics[param.find('th').text.lower()] = param.find('td').text

        return characteristics

    @classmethod
    async def parse_item(cls, link: str, session: ClientSession, brand: str) -> Car:
        html: Optional[BeautifulSoup]
        characteristics: Dict[str, str]

        html = await cls._get_page(link, session)
        characteristics = cls._extract_characteristics(html)

        return Car(
            model=cls._extract_model(html, brand),
            year=cls._extract_year(html),
            price=cls._extract_price(html),
            color=characteristics['цвет'],
            mileage=re.sub(r'\D', '', characteristics['пробег, км']),
            engine=characteristics['двигатель'],
            horse_power=re.sub(r'\D', '', characteristics['мощность']),
            transmission=characteristics['коробка передач'],
        )

    @classmethod
    async def parse_items_by_links(cls, item_links: List[str], session: ClientSession, brand: str) -> List[Car]:
        tasks: List[Any]
        data: List[Car]

        tasks = [cls.parse_item(item_link, session, brand) for item_link in item_links]
        data = await asyncio.gather(*tasks)

        return data


class Drom:

    @staticmethod
    async def check_brand(brand: str) -> bool:
        url: str

        async with ClientSession() as session:
            url = f'{config.DOMAIN}{brand.lower()}/'
            async with session.get(url) as resp:
                if resp.status == 200:
                    return True
                return False

    @staticmethod
    async def _find_num_pages(brand: str) -> int:
        step: int
        cur_page: int
        visited: Optional[int]

        step = 20
        cur_page = 1
        visited = None
        async with ClientSession() as session:
            while not (await DromPagination.check_page(cur_page, session, brand)
                       and not await DromPagination.check_page(cur_page + 1, session, brand)):
                if not await DromPagination.check_page(cur_page, session, brand):
                    visited = cur_page
                    cur_page -= step
                    step = step // 2
                if cur_page + step == visited:
                    step = step // 2
                cur_page += step

        return cur_page

    @staticmethod
    def save_items(items: List[Car], brand: str) -> None:
        ind: int

        with open(f'results/{brand}.csv', 'w', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=';')
            csv_writer.writerow([
                '№', 'Model', 'Year', 'Color', 'Price',
                'Mileage(km)', 'Engine', 'Horse Power', 'Transmission'
            ])
            ind = 1
            for item in items:
                if item.year + item.price + item.mileage != '':
                    csv_writer.writerow([
                        ind, item.model, item.year, item.color, item.price,
                        item.mileage, item.engine, item.horse_power, item.transmission
                    ])
                    ind += 1

        return

    @classmethod
    async def parse(cls, brand: str) -> None:
        brand: str
        num_pages: int
        item_links: List[str]
        items: List[Car]

        brand = brand.lower()
        num_pages = await cls._find_num_pages(brand)
        async with ClientSession() as session:
            item_links = await DromPagination.get_item_links(session, brand, num_pages)
            items = await DromItem.parse_items_by_links(item_links, session, brand)
        cls.save_items(items, brand)

        return
