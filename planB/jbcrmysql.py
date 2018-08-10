# -*- coding: utf-8 -*-
import pymysql
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Float, Integer,CHAR,INT,DATE
import os
import io as cStringIO
import datetime

def insertmysql():
    '''
    insert into text values(‘ji’,’fsd’),(‘sfd’,’fsdf’);
    将dataframe分解成以上格式，可以部分插入和全部插入
    效率低下，用load data infile
    '''
    databasename = 'kline'
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
    db=databasename)
    cursor=conn.cursor()
    
    path = 'D:/py/resource/basicdata/kline/'
    columnslist = ['date','code','name','close','high','low','open','prevopen','price_change','percent_change',
        'turnover','volume','amount','market_value','c_market_value']
    tablecolumns = '(date,code,name,close,high,low,open,prevopen,price_change,percent_change,turnover,volume,amount,market_value,c_market_value)'
    #j ='num INT NOT NULL AUTO_INCREMENT,'
    j = ''
    for i in columnslist:
        if i == 'date' :
            i = i + ' date NOT NULL,'
            j = j + i
        elif  i == 'code' or i == 'name':
            i = i + ' VARCHAR(20) NOT NULL,'
            j = j + i
        elif i == 'volume':
            i = i + ' bigint(20),'
            j = j + i
        elif i =='market_value' or i =='amount' or i == 'c_market_value':
            i = i + ' double NOT NULL,'
            j = j + i
        else:
            i = i + ' float(8) not null,'
            j = j + i        
    cl = '('+j+'PRIMARY KEY (date))'  
    #UNIQUE KEY (date)
    for cf in os.listdir(path):
        code = cf.split('.')[0]
        filepath = path + cf
        df1 = pd.read_csv(filepath,encoding="GB18030")
        df1.columns= columnslist
        #df1.fillna(0.0)#没办法将None改为数字
        df1['percent_change'] = df1['percent_change'].replace('None',0.0)
        df1['price_change'] = df1['price_change'].replace('None',0.0)
        df1['turnover'] = df1['turnover'].replace('None',0.0)
        df1[['percent_change','price_change','turnover']] = df1[['percent_change','price_change','turnover']].astype(float)
        df1 = df1.sort_values('date')
        df1 = df1.reset_index()
        del df1['index']
        
        table_name = 'k_'+ code 
        
        sql1 = 'insert into '+ table_name + ' ' +tablecolumns+' values '
        for i in range(0,len(df1)):
            sql1 = sql1 + '('
            sql1 = sql1 + '"'+df1['date'][i]+'",'
            sql1 = sql1 + '"'+df1['code'][i]+'",'
            sql1 = sql1 + '"'+df1['name'][i]+'",'
            sql1 = sql1 + str(df1['close'][i]) +','
            sql1 = sql1 + str(df1['high'][i]) +','
            sql1 = sql1 + str(df1['low'][i]) +','
            sql1 = sql1 + str(df1['open'][i]) +','
            sql1 = sql1 + str(df1['prevopen'][i]) +','
            sql1 = sql1 + str(df1['price_change'][i]) +','
            sql1 = sql1 + str(df1['percent_change'][i]) +','
            sql1 = sql1 + str(df1['turnover'][i]) +','
            sql1 = sql1 + str(df1['volume'][i]) +','
            sql1 = sql1 + str(df1['amount'][i])+',' 
            sql1 = sql1 + str(df1['market_value'][i])+','
            sql1 = sql1 + str(df1['c_market_value'][i])
            if i == len(df1)-1:
                sql1 = sql1 + ')'
            else:
                sql1 = sql1 + '),'
        cursor=conn.cursor()
        create='create table if not exists '+ table_name + cl + ' DEFAULT CHARSET=UTF8MB4'
        cursor.execute(create)
        cursor.execute(sql1)
        conn.commit()
        #建立表格
        cursor.close()
        conn.close()
        print('done')


if __name__ == '__main__':
    insertmysql()