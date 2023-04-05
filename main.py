import asyncio
import datetime

from crawler import orm
from crawler import crawler


async def async_main():
    orm.create_all()

    cr = crawler.Crawler(
        date_from=datetime.date(1992, 7, 1),
        # date_to=datetime.date(1992, 7, 2),
    )
    await cr.go()


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
