from datetime import date
import datetime


class Customer:
    def __init__(self, customerId, cityCode, dateOfBirth, phoneNumber):
        self.customerId = customerId
        self.cityCode = cityCode
        self.dateOfBirth = dateOfBirth
        self.phoneNumber = phoneNumber

    def printCustomer(self):
        print(self.customerId, self.cityCode, self.dateOfBirth)

    def calculateAge(self):
        days_in_year = 365.2425
        date_time_obj = datetime.datetime.strptime(self.dateOfBirth, '%Y-%m-%d')
        age = int((date.today() - date_time_obj).days / days_in_year)
        return age
