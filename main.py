import asyncio

from src.core.structure import Drom


async def main() -> None:
    # ask to enter car brand
    brand = input('Enter car brand: ')

    # ask brand until it exists
    while not await Drom.check_brand(brand):
        print('Invalid brand')
        # ask to provide car brand again
        brand = input('Enter car brand: ')

    # parse all cars under specified brand
    await Drom.parse(brand)

    return


if __name__ == '__main__':
    asyncio.run(main())
