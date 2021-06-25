from logging import error
from utils.db import DataBase


class Services:
    def save_funding(self, exchange, date, asset, future, type, rate):
        sql = '''insert into crypto.fundings(exchange, date, asset, future, type, rate) values ($1, $2, $3, $4, $5, $6);'''
        args = [exchange, date, asset, future, type, rate]
        try:
            return DataBase.query(sql, args)
        except:
            return 'Error saving fundings'

    def get_load_date(self):
        sql = '''select max(date) from crypto.fundings;'''
        try:
            return DataBase.query(sql)
        except:
            return 'Error saving fundings'
