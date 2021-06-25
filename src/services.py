from datetime import datetime
from logging import error
from utils.db import DataBase


class Services:
    def save_funding(self, exchange, date, asset, future, type, rate):
        sql = '''insert into crypto.fundings(exchange, date, asset, future, type, rate) values ($1, $2, $3, $4, $5, $6);'''
        args = [exchange, date, asset, future, type, rate]
        try:
            return DataBase.query(sql, args)
        except Exception as e:
            print(e)

    def get_load_date(self):
        sql = '''select date(max(date)) as last_date from crypto.fundings;'''
        try:
            result = DataBase.query(sql, None)
            date_str = (result.get('rows')[0]).get('last_date', '1900-01-01').split('T')[0]
            last_date = datetime.strptime(date_str, '%Y-%m-%d')
            return last_date
        except Exception as e:
            print(e)
