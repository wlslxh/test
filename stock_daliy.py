#!/usr/bin/env python
#coding: utf-8
#
import sys, urllib, json,  threading, datetime, time
import MySQLdb as mysql

import os, struct
from datetime import datetime
class Stock(threading.Thread):

    LAST = 19700101

    def __init__(self,market = None, path = None, prefix = None):
        self.market = market
        self.path = path
        self.prefix = prefix
        self.codes = []
        self.daliy = []
        threading.Thread.__init__(self)

    #获取日线历史数据文件
    def get_stock_codes(self):
        self.codes = [ f for f in os.listdir(self.path) if f.endswith('.day') and f.startswith(self.prefix)]


    def get_daliy_data(self,code):
        daliy = "{0}{1}".format(self.path,code)
        print('开始解析数据({0})'.format(daliy))
        f = open(daliy,'rb')

        while 1:
            data = f.read(32)
            if len(data) == 0:
                break

            daliy_data = tuple(struct.unpack('iiiiifii',data)) + (code[2:8],)
            #daliy_data = struct.unpack('IIIIIfII', data[1,32])
            #只有提取日期大于已保存的最大日期
            #print(type(Stock.LAST), (daliy_data[0]))
            if daliy_data[0] > Stock.LAST:
                self.daliy.append(daliy_data)  

    #获取已存取的最大日期
    #如果日期不存在返回默认值19700101
    @staticmethod
    def get_last_daliy():
        db = mysql.connect(host = 'localhost', user = 'root', passwd = 'root', db = 'stock', charset='utf8')
        cursor = db.cursor()
        try:
            sql = '''SELECT MAX(st_date) FROM daliy'''
            cursor.execute(sql, )
            result = cursor.fetchone()
            #print result
            Stock.LAST = 19700101 if result[0] == None else int(result[0].strftime('%Y%m%d'))
        except:
            pass
        cursor.close()
        db.close()

    #st_date, st_open, st_high, st_low, st_close, st_amount, st_vol, st_reservation = struct.unpack('iiiiifii',data)    
    #print st_date, code[2:8], float(st_open)/100, float(st_high)/100, float(st_low)/100, float(st_close)/100, st_amount, st_vol 
    def set_daliy_data(self):

        db = mysql.connect(host = 'localhost', user = 'root', passwd = 'root', db = 'stock', charset='utf8')
        cursor = db.cursor()
        
        sql = '''INSERT INTO daliy(st_date,st_open,st_high,st_low,st_close,st_amount,st_vol,st_reservation,st_code) 
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        for d in self.daliy:
            try:
                if d[0] > Stock.LAST:
                    args = ('{0}-{1}-{2}'.format(str(d[0])[0:4], str(d[0])[4:6], str(d[0])[6:8]), 
                        float(d[1])/100, float(d[2])/100, float(d[3])/100, float(d[4])/100, 
                        d[5], d[6], d[7], d[8])
                    #print args
                    cursor.execute(sql, args)
                    db.commit()
            except:
                db.rollback()
        cursor.close()
        db.close()


    #获取日线数据
    def set_receiver(self):
        for code in self.codes:
            self.get_daliy_data(code)
            #print code

    def run(self):
        print('获取日线历史数据文件({0})'.format(self.prefix))
        self.get_stock_codes()
        #self.get_last_daliy()
        print('获取日线数据({0})'.format(self.prefix))
        #print('获取日线数据({0})'.format(self.codes))
        self.set_receiver()
        print('开始保存历史数据({0})'.format(self.prefix))
        self.set_daliy_data()  



def main():

    Stock.get_last_daliy()
    print('最后同步日期({0})'.format(Stock.LAST));

    threads = []
    threads.append(Stock('sh','c:/new_tdx_cfv/vipdoc/sh/lday/','sh68859'))
    threads.append(Stock('sz','c:/new_tdx_cfv/vipdoc/sz/lday/','sz12318'))

    for t in threads:
        t.start()
    for t in threads:
        t.join()    

    print('SUCCESS: DATA RECEIVED')


if __name__ == '__main__':
    main()