import logging
import os
import time

from sqlalchemy import Column
from sqlalchemy import DECIMAL
from sqlalchemy import Date
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base

Base = declarative_base()

_DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')
assert _DB_CONNECTION_STRING, 'Connection string is empty'
engine = create_engine(_DB_CONNECTION_STRING, future=True)

logger = logging.getLogger(__name__)


class CollectedDataRaw(Base):
    __tablename__ = "exchange_rates_raw"
    id = Column(Integer, primary_key=True)
    date = Column(Date(), nullable=False)
    num_code = Column(String(), nullable=False)
    str_code = Column(String(), nullable=False)
    quantity = Column(Integer(), nullable=False)
    name = Column(String(), nullable=False)
    value = Column(DECIMAL(20, 4), nullable=False)

    __table_args__ = (
        UniqueConstraint('date', 'name', name='exchange_rates_raw_date_currency_unique'),
    )

    def __repr__(self):
        return f"CollectedDataRaw(id={self.id!r})"


def create_all(force_recreate=False):
    print('Start creating all...')
    if force_recreate:
        print('[WARNING] About to DROP and then create all tables...')
        for i in range(5, 0, -1):
            print(f'{i}...')
            time.sleep(1)

        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print('Done!')
