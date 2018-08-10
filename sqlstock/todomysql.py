# -*- coding: utf-8 -*-
import pymysql
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Float, Integer,CHAR,INT
import os
import io as cStringIO
import datetime



'''
    #load data infile 'F:/MySqlData/test1.csv' --CSV文件存放路径
    #into table student character set gb2312 --要将数据导入的表名,含有中文
    #fields terminated by ',' optionally enclosed by '"' escaped by '"'
    #lines terminated by '\r\n';
    #sql = 'show columns from sh000002'
    #cur.execute(sql)
    #data = cur.fetchall()
    data = conn.execute("SELECT * FROM sh000002 limit 5").fetchall()
    print data
    sql1 = 'select * from sh000002 ;'
    df = pd.read_sql(sql1,con = conn)
    print df.head(5)
    del df['num']
    df.to_csv('d:/123.csv',encoding = "GB18030",index=False)
'''
def fast():
    time1 = datetime.datetime.now()
    databasename = 'text'
    db = pymysql.connect('localhost', 'root', 'hunter123', charset='UTF8MB4')
    cursor = db.cursor()
    cursor.execute('create database if not exists '+ databasename)
    cursor.close()
    db.close()
    conn=pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    passwd='hunter123',
    db=databasename,local_infile=1)
    cursor=conn.cursor()
    path = 'D:/py/stockdata/kline/'
    columnslist = ['date','code','name','close','high','low','open','prevopen','price_change','percent_change',
        'turnover','volume','amount','market_value','c_market_value']
    j =''
    for i in columnslist:
        if i == 'date' :
            i = i + ' date NOT NULL,'
            j = j + i
        elif i == 'code' or i == 'name':
            i = i + ' VARCHAR(20) NOT NULL,'
            j = j + i
        elif i == 'volume':
            i = i + ' bigint(20),'
            j = j + i
        elif i =='market_value' or i =='amount':
            i = i + ' double NOT NULL,'
            j = j + i
        elif i == 'c_market_value':
            i = i + ' double NOT NULL'
            j = j + i            
        else:
            i = i + ' float(8) not null,'
            j = j + i        
    cl = '('+j+')'
    #cl = '('+j+'UNIQUE KEY (date))'   
    #print(cl) 
    m = 1
    for cf in os.listdir(path):
        code = cf.split('.')[0]
        table_name = 'k_'+ code
        filepath = path + cf
        df1 = pd.read_csv(filepath,encoding="GB18030")
        df1.columns= columnslist
        df1.fillna(0.0)#没办法将'None'字符串改为数字
        df1['percent_change'] = df1['percent_change'].replace('None',0.0)
        df1['price_change'] = df1['price_change'].replace('None',0.0)
        df1['turnover'] = df1['turnover'].replace('None',0.0)
        df1[['percent_change','price_change','turnover']] = df1[['percent_change','price_change','turnover']].astype(float)
        df1 = df1.sort_values('date')
        df1 = df1.reset_index()
        del df1['index']
        csv_filename = 'D:/py/stockdata/text/'+table_name+'.csv'
        df1.to_csv(csv_filename,encoding = 'UTF-8',index = False)

        cursor=conn.cursor()
        create='create table if not exists '+ table_name + cl + ' DEFAULT CHARSET=UTF8MB4'
        dataload1 = 'LOAD DATA local INFILE '+'"'+csv_filename+'"'+' ignore INTO TABLE '+table_name 
        #ignore INTO 或者 replace into 用于存在相同文件名时的办法，如果不加，会自动append，但如果
        #出现unique项目有重复项，会自动中断这次导入。
        dataload2 = ' character set UTF8MB4 ' 
        dataload3 = '''FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\\r\\n' ignore 1 lines'''
        dataload = dataload1+dataload2+dataload3
        uniquecol = 'ALTER TABLE '+ table_name+' ADD unique(date);'
        cursor.execute(create)
        cursor.execute(dataload)
        cursor.execute(uniquecol)
        conn.commit()
        print(code+' done number %d'%m)
        m = m + 1
    cursor.close()
    conn.close()
    print('alldone')
    time2 = datetime.datetime.now()
    print(time1)
    print(time2)
    print(time2-time1)
    '''
    LOAD DATA INFILE 'csv_file'
    IGNORE INTO TABLE table_name
    CHARACTER SET UTF8
    FIELDS TERMINATED BY ';'
    OPTIONALLY ENCLOSED BY '"' Escaped By '"'
    LINES TERMINATED BY '\n'
    '''

if __name__ == '__main__':
    fast()
    

