import asyncio
import datetime
import itertools
import logging

from sqlalchemy import desc
from sqlalchemy.orm import Session

from . import api_client
from . import orm

_MIN_DATE = datetime.date(1992, 7, 1)
_MAX_CONCURRENT = 1000


class NotValidDate(ValueError):
    pass


logger = logging.getLogger(__file__)


class Crawler:
    def __init__(self, *, date_from=_MIN_DATE, date_to=None, last_date_from_db=True):
        self._date_from = self._get_validated_date_from(date_from, last_date_from_db)
        self._date_to = self._get_validated_date_to(date_to)

    @staticmethod
    def _get_validated_date_from(date, last_date_from_db):
        if date < _MIN_DATE:
            raise NotValidDate(f'`date_from` must be after {_MIN_DATE}')
        if last_date_from_db:
            date = max(date, Crawler._get_last_date_from_db())
        return date

    @staticmethod
    def _get_last_date_from_db():
        with Session(orm.engine) as session:
            max_obj = session.query(orm.CollectedDataRaw).order_by(desc(orm.CollectedDataRaw.date)).first()
            max_date = max_obj.date if max_obj else _MIN_DATE
        return max_date

    @staticmethod
    def _get_validated_date_to(date):
        # TODO (ojgen): Fuck timezones for now
        today = datetime.datetime.now().date()
        if date is None:
            return today
        if date > today:
            raise NotValidDate(f'`date_to` must be before {today}')
        return date

    @staticmethod
    def _convert_to_orm_model(exchange_rate):
        new = orm.CollectedDataRaw(
            date=exchange_rate.date,
            num_code=exchange_rate.num_code,
            str_code=exchange_rate.str_code,
            quantity=exchange_rate.quantity,
            name=exchange_rate.name,
            value=exchange_rate.value,
        )
        return new

    async def _process_date(self, date):
        async with api_client.ApiClient() as client:
            rates = await client.get_on_date(date=date)
        rates_as_orm_models = [self._convert_to_orm_model(r) for r in rates]
        with Session(orm.engine) as session:
            with session.begin():
                session.add_all(rates_as_orm_models)
                session.commit()

    async def go(self):
        coroutines = []
        curr_date = self._date_from
        while curr_date <= self._date_to:
            curr_date += datetime.timedelta(days=1)
            coroutines.append(self._process_date(curr_date))

        logger.info(f'Total dates: {len(coroutines)}')
        for i, batch in enumerate(batched_it(coroutines, _MAX_CONCURRENT)):
            logger.info(f'Start processing batch #{i+1}')
            await asyncio.gather(*batch)

        logger.info(f'Finished processing!')


def batched_it(iterable, n):
    "Batch data into iterators of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)
