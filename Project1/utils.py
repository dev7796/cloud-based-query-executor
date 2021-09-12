import pymysql
import psycopg2
from time import time


class connect_mysql():
    def __init__(self, host="localhost", user="root", password='Gladiators7', db="my_first_database"):
        self.db = pymysql.connect(host, user, password, db)
        self.cursor = self.db.cursor()

    def run_query(self, query_statement):
        start_time = int(round(time() * 1000))
        self.cursor.execute(query_statement)
        col_info = self.cursor.description
        result = self.cursor.fetchall()
        query_time = str(int(round(time() * 1000)) - start_time) + " ms"
        if len(result) > 1000:
            result = result[:99]
            query_time += '\nThe result is too larger to transmit and display, so we limit the size to return'
        col_name = []
        for i in range(len(col_info)):
            col_name.append(col_info[i][0])
        return col_name, result, query_time

    def disconnect(self):
        self.db.close()


class connect_redshift():
    def __init__(self, host="loaclhost", database='database', user='awsuser', password='Cs123456', port=5439):
        self.con = psycopg2.connect(host=host, dbname=database, port=port, password=password, user=user)
        self.cur = self.con.cursor()

    def run_query(self, query_statement):
        start_time = int(round(time() * 1000))
        self.cur.execute(query_statement)
        col_info = self.cur.description
        result = self.cur.fetchall()
        query_time = str(int(round(time() * 1000)) - start_time) + " ms"
        if len(result) > 1000:
            result = result[:99]
            query_time += '\nThe result is too larger to transmit and display, so we limit the size to return'
        col_name = []
        for i in range(len(col_info)):
            col_name.append(col_info[i][0])
        return col_name, result, query_time

    def disconnect(self):
        self.cur.close()
        self.con.close()
