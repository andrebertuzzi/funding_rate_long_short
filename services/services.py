from logging import error
from ..db import DataBase


class Services:
    @staticmethod
    def save_funding(exchange, date, asset, future, type, rate):
        sql = '''insert into crypto.fundings(exchange, date, asset, future, type, rate) values ($1, $2, $3, $4, $5, $6);'''
        args = [exchange, date, asset, future, type, rate]
        try:
            return DataBase.query(sql, args)
        except:
            error('Error saving fundings')
