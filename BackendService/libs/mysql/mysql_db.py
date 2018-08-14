# -*- coding: utf-8 -*-

import mysql.connector
from mysql.connector import errorcode
import logging


class MySQLBase:
    error_msg = ''
    is_debug = False


    def __init__(self, host='localhost', user='wanplus', password='', database='', port=3306, charset=None):
        """init mysql connect"""
        try:
            self.__conn = mysql.connector.connect(host=host, user=user, password=password, database=database, port=port)
            self.__cursor = self.__conn.cursor()
            if charset:
                if len(charset) > 1:
                    self.__conn.set_charset_collation(charset[0], charset[1])
                else:
                    self.__conn.set_charset_collation(charset[0])
        except mysql.connector.Error as err:
            self.error_msg = "MySQL error, msg: {}".format(err)

        self.is_debug = False

    def ready(self):
        return hasattr(self, '_MySQLBase__cursor')

    def fetch_more(self, table='', fields='*', where='', order='', limit=0, size=0):


        sql = self.build_sql(table, fields, where, order, limit, size)
        result = self.query(fields, sql)

        # print result
        if type(result) is int:
            self.error_msg = "MySQL error, fetch_more is fail, sql:%s" % sql
            return -100
        return result

    def fetch_one(self, table='', fields='', where='', order=''):

        sql = self.build_sql(table, fields, where, order, 1)
        result = self.query(fields, sql)
        if type(result) is int:
            self.error_msg = "MySQL error, fetch_one is fail, sql:%s" % sql
            return -101
        return result[0] if result else None

    def count(self, table, where):

        sql = self.build_sql(table, 'COUNT(*)', where, '', 1)
        if not self.__execute(sql):
            self.error_msg = "MySQL error, count is fail, sql:%s" % sql
            return -102
        result = self.__cursor.fetchone()
        return result[0] if result else 0

    def insert(self, table, data):
      
        fields = ','.join(data.keys())  # 
        inputs = ','.join(("%s", ) * len(data))  # 
        values = tuple(data.values())  # value
        sql = "INSERT INTO %s (%s) VALUES (" % (table, fields) + inputs + ")"
        if not self.__execute(sql, values):
            self.error_msg = "insert, msg:%s" % self.error_msg
            return -103
        insert__id = self.__cursor.lastrowid
        self.__conn.commit()
        return insert__id

    def multi_insert(self, table, data):
 
        fields = ','.join(data[0].keys())
        inputs = ','.join(("%s", ) * len(data[0]))
        values = []
        [values.append(tuple(item.values())) for item in data]

        sql = "INSERT INTO %s (%s) VALUES (" % (table, fields) + inputs + ")"
        self.__cursor.executemany(sql, values)
        self.__conn.commit()
        return self.__cursor.rowcount

    def update(self, table, data, where):

        fields = (",".join(map(lambda k: k + "=%s", data.keys())))
        values = tuple(data.values())
        sql = "UPDATE %s SET " % table + fields + " WHERE " + where
        if not self.__execute(sql, values):
            self.error_msg = "MySQL error, execute is fail, sql:" + sql
            return -105
        self.__conn.commit()
        return self.__cursor.rowcount

    def incr(self, table, field, where, unit = 1):

        sql = "UPDATE %s SET %s = %s + %d" % (table, field, field, unit) + " WHERE " + where
        if not self.__execute(sql):
            self.error_msg = "MySQL error, execute is fail, sql:" + sql
            return -105
        self.__conn.commit()
        return self.__cursor.rowcount

    def decr(self, table, field, where, unit = 1):
    
        sql = "UPDATE %s SET %s = %s - '%d'" % (table, field, field, unit) + " WHERE " + where
        if not self.__execute(sql):
            self.error_msg = "MySQL error, execute is fail, sql:" + sql
            return -105
        self.__conn.commit()
        return self.__cursor.rowcount

    def delete(self, table, where):

        where = ' WHERE ' + where if where else ''
        sql = 'DELETE FROM ' + table + where
        if not self.__execute(sql):
            self.error_msg = "MySQL error, execute is fail,sql:" + sql
            return -106
        self.__conn.commit()
        return self.__cursor.rowcount

    def close(self):

        if not hasattr(self, '_MySQLBase__cursor'):
            self.error_msg = "MySQL error, __cursor is None"
            return -100
        self.__cursor.close()
        self.__conn.close()

    @staticmethod
    def build_sql(table='', fields='', where='', order='', limit=0, size=0):
 
        where = ' WHERE ' + where if where else ''
        limit = ' LIMIT ' + str(limit) if limit else ''
        limit += ',' + str(size) if limit and size else (' LIMIT ' + str(size) if size else '')
        order = ' ORDER BY ' + order if order else ''
        fields = fields if fields else '*'
        sql = 'SELECT ' + fields + ' FROM ' + table + where + order + limit

        return sql

    def query(self, fields, sql):
 
        if type(self.__execute(sql)) == int:
            self.error_msg = "MySQL error, execute is fail, sql:" + sql
            return -109
        result = []
        column_names = self.__cursor.column_names if not fields or fields == '*' else tuple(fields.split(','))
        [result.append(dict(zip(column_names, item))) for item in self.__cursor]

        return result

    def __execute(self, sql, values=None):
    
        # print sql
        try:
            if not hasattr(self, '_MySQLBase__cursor'):
                return -110

            if values:
                self.__cursor.execute(sql, values)
            else:
                self.__cursor.execute(sql)
            return True
        except mysql.connector.Error as err:
            self.error_msg = "MySQL __execute error, msg: {}, sql: {}".format(err, sql)
            logging.error(self.error_msg)
            return err.errno  # error number

    def get_error_msg(self):
        return self.error_msg

    def set_debug(self, is_debug=False):
        self.is_debug = is_debug
        print(self.is_debug)

    def multi_replace(self, table, data, num=10000):

        fields = ','.join(data[0].keys())
        inputs = ','.join(("%s",) * len(data[0]))
        values = []

        sql = "REPLACE INTO %s (%s) VALUES (" % (table, fields) + inputs + ")"
        for item in data:
            values.append(tuple(item.values()))
            if len(values) == num:
                try:
                    self.__cursor.executemany(sql, list(values))
                    values = []
                except mysql.connector.Error as err:
                    self.err__msg = "MySQL error, msg: {}".format(err)
                    return -102
        self.__cursor.executemany(sql, values)
        self.__conn.commit()

    def start_transaction(self):
        self.__conn.start_transaction()

    def commit(self):
        self.__conn.commit()

    def rollback(self):
        self.__conn.rollback()

    def transaction_insert(self, table, data):
  
        fields = ','.join(data.keys())  # 字符
        inputs = ','.join(("%s", ) * len(data))  # 参数
        values = tuple(data.values())  # value
        sql = "INSERT INTO %s (%s) VALUES (" % (table, fields) + inputs + ")"
        if type(self.__execute(sql, values)) is int:
            self.error_msg = "insert, msg:%s" % self.error_msg
            return -103
        return self.__cursor.lastrowid

    def transaction_update(self, table, data, where):

        fields = (",".join(map(lambda k: k + "=%s", data.keys())))
        values = tuple(data.values())
        sql = "UPDATE %s SET " % table + fields + " WHERE " + where
        if type(self.__execute(sql, values)) is int:
            self.error_msg = "MySQL error, execute is fail, sql:" + sql
            return -105
        return self.__cursor.rowcount

    def transaction_incr(self, table, field, where, unit = 1):
 
        sql = "UPDATE %s SET %s = %s + %d" % (table, field, field, unit) + " WHERE " + where
        if type(self.__execute(sql)) is int:
            self.error_msg = "MySQL error, execute is fail, sql:" + sql
            return -105
        return self.__cursor.rowcount

    def transaction_decr(self, table, field, where, unit = 1):
  
        sql = "UPDATE %s SET %s = %s - '%d'" % (table, field, field, unit) + " WHERE " + where
        if type(self.__execute(sql)) is int:
            self.error_msg = "MySQL error, execute is fail, sql:" + sql
            return -105
        return self.__cursor.rowcount