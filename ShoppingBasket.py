import datetime
import json
import random
import time
from random import randint
from Customer import Customer
from Product import Product
from ShoppingBasketModel import ShoppingBasketModel


def randomDate():
    start_date = datetime.date(1960, 1, 1)
    end_date = datetime.date(2002, 1, 1)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    return random_date


def random_with_N_digits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return randint(range_start, range_end)


# https://stackoverflow.com/questions/7963762/what-is-the-most-economical-way-to-convert-nested-python-objects-to-dictionaries
def to_dict(obj):
    return json.loads(json.dumps(obj, default=lambda o: o.__dict__))


def RandomShoppingBasket():
    product_list = ['ice tea', 'water', 'lemonade', 'orange juice', 'peach juice', ' cheese', 'olive', 'butter',
                    'honey', 'rice', 'pasta', 'lentil', 'chickpeas', 'sugar', 'salt', 'baking powder', 'vanilla',
                    'cocoa', 'cucumber', 'parsley', 'ginger', 'potato', 'onion', 'tomato', 'dish soap', 'shampoo']
    product_cost_list = [4.25, 1.25, 2.85, 5.75, 3.25, 15.50, 14.00, 26.00, 40.00, 8.00, 4.50, 17.75, 15.25,
                         12.00, 10.00, 2.00, 2.15, 4.50, 3.50, 3.25, 4.50, 5.00, 4.75, 5.50, 22.50, 12.00]
    # current time
    current_time = datetime.datetime.now()
    str_current_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    # str_current_time = str(current_time)

    # customer info
    customerId = random.randint(1, 100)
    cityCode = random.randint(1, 81)
    date = randomDate()
    strDate = "{:%Y-%m-%d}".format(date)
    phoneNumber = random_with_N_digits(10)

    customer = Customer(customerId, cityCode, strDate, phoneNumber)
    # product info
    productId = random.randint(1, 20)
    productName = product_list[productId]
    productCost = product_cost_list[productId]
    product = Product(productId, productName, productCost)
    # shopping basket
    productCount = random.randint(1, 10)
    shoppingBasket = ShoppingBasketModel(customer, product, str_current_time, productCount)
    # json
    jsonShoppingBasket = to_dict(shoppingBasket)
    return jsonShoppingBasket
