import asyncio
import datetime
import decimal
import logging
import typing

import aiohttp
import python_socks
import bs4

from . import models

_BASE_URL = 'https://www.cbr.ru/currency_base/daily/'
_DATE_FORMAT = '%d.%m.%Y'

_REQUEST_ATTEMPTS = 5


class APIProblem(RuntimeError):
    pass


class APINegativeResponse(RuntimeError):
    pass


logger = logging.getLogger(__file__)


class ApiClient:
    _session: aiohttp.ClientSession

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.close()

    async def get_on_date(self, date: datetime.date) -> typing.List[models.ExchangeRateRaw]:
        params = {
            'UniDbQuery.Posted': 'True',
            'UniDbQuery.To': date.strftime(_DATE_FORMAT)
        }
        text = await _make_safe_request(self._session, _BASE_URL, 'GET', params=params)
        rates = _parse_currencies_html(text, date)
        return rates


def _calculate_delay(attempt: int):
    max_delay = 3
    sleep = min(
        0.2 * (2**attempt),
        max_delay,
    )
    return sleep


async def _make_safe_request(session: aiohttp.ClientSession, url: str, method: str, **kwargs: typing.Dict):
    for i in range(_REQUEST_ATTEMPTS):
        try:
            async with session.request(method, url=url, **kwargs) as response:
                logger.info(
                    f'Response status={response.status}, '
                    f'url={response.url}',
                )
                if not (200 <= response.status <= 299):
                    raise APINegativeResponse(
                        f'Status: {response.status}, '
                        f'text: {await response.text()}'
                    )
                data = await response.text()
                return data
        except (aiohttp.ClientConnectionError, python_socks.ProxyError):
            sleep = _calculate_delay(i)
            logger.warning(
                f'Cannot fetch data. Current attempt is {i + 1}. '
                f'Sleep: {sleep:.1f} sec.',
            )
            await asyncio.sleep(sleep)
    raise APIProblem('Cannot fetch from cbr.ru')


def _parse_currencies_html(text: str, date: datetime.date):
    soup = bs4.BeautifulSoup(text, 'html.parser')
    rates_table = soup.find_all('table', 'data')

    if not rates_table:
        logger.warning(
            f'Cannot find exchange rates data table',
        )
        return []

    result = []
    for row in rates_table[0].tbody.find_all('tr')[1:]:
        data = [column.string for column in row.find_all('td')]
        result.append(
            models.ExchangeRateRaw(
                date=date,
                num_code=data[0],
                str_code=data[1],
                quantity=int(data[2]),
                name=data[3],
                value=decimal.Decimal(data[4].replace(' ', '').replace(',', '.')),
            )
        )
    return result
