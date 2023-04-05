import dataclasses
import datetime
import decimal


@dataclasses.dataclass
class ExchangeRateRaw:
    date: datetime.date
    num_code: int
    str_code: str
    quantity: int
    name: str
    value: decimal.Decimal
