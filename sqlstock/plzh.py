# -*- coding: utf-8 -*-
from stockrun.standard import standard_stock as ss
import pandas as pd
import datetime

def goodcolumns(df):
    columnslist = []
    for i in df.columns:
        if u'报告' in i:
            columnslist.append('date')
        elif u'Unnamed' in i:
            del df[i]
        else:
            columnslist.append(i)
    df.columns = columnslist
    return df 

def goodindex(df,lable):
    dr = pd.read_csv('D:/py/stockdata/cwzb.csv',encoding="GB18030")
    dr = dr[dr.lab == lable]
    indexlist = []   
    for i in dr['name']:
        indexlist.append(i)
    del indexlist[0]
    df.index = indexlist
    return df


def hbzycb():
    codelist = ss.codelist()
    #codelist =['000002']
    path = 'D:/py/stockdata/'
    hbpath = 'D:/py/stockdata/to_mysql_zycb/'
    for code in codelist:
        zycwzbpath = path + 'zycwzb/'+code+'.csv'
        ylnlpath = path + 'ylnl/'+code+'.csv'
        cwbbzypath = path + 'cwbbzy/'+code+'.csv'
        chnlpath = path + 'chnl/'+code+'.csv'
        cznlpath = path + 'cznl/'+code+'.csv'
        yynlpath = path + 'yynl/'+code+'.csv'
    
        df1 = pd.read_csv(zycwzbpath,encoding='GB18030')
        df1 = goodcolumns(df1)
        df1 =goodindex(df1,'zycwzb')
        
        
        df2 = pd.read_csv(ylnlpath,encoding='GB18030')
        df2 = goodcolumns(df2)
        df2 = goodindex(df2,'ylnl')
        
        
        df = df1.append(df2)

       
        df1 = pd.read_csv(cwbbzypath,encoding='GB18030')
        df1 = goodcolumns(df1)
        df1 =goodindex(df1,'cwbbzy')
        #df1.rename(columns = {'报告期':'报告日期'}, inplace = True)
        df = df.append(df1)
      
        df1 = pd.read_csv(cznlpath,encoding='GB18030')
        df1 = goodcolumns(df1)
        df1 =goodindex(df1,'cznl')
        
        df2 = pd.read_csv(yynlpath,encoding='GB18030')
        df2 = goodcolumns(df2)
        df2 =goodindex(df2,'yynl')

        df = df.append(df1)
        df = df.append(df2)
        
        df1 = pd.read_csv(cwbbzypath,encoding='GB18030')
        df1 = goodcolumns(df1)
        df1 =goodindex(df1,'cwbbzy')
        df2 = pd.read_csv(chnlpath,encoding='GB18030')
        df2 = goodcolumns(df2)
        df2 =goodindex(df2,'chnl')

        df = df.append(df1)
        df = df.append(df2)

        df = df.drop_duplicates(['date'])
        df.to_csv('d:/123.csv',encoding = 'GB18030')
        del df['date']
        dft = df.T
        dft.fillna(0.0)
        '''
        cl = []
        cc = dft.irow[0]
        #试过dft[0:2]等方法 都没有用,data.icol(0)   #取data的第一列,,data.irow(0)取第一行  
        for i in cc:
            cl.append(i)
        dft.columns = cl
        dft = df.drop(0)
        '''
        
        dft['date'] = dft.index
        dft.to_csv(hbpath+code+'.csv',index = False,encoding = 'GB18030')
        print dft
        
        '''
        dft = df.T
        dft = dft.reset_index()
        cl = []
        cc = dft.iloc[0]
        #试过dft[0:2]等方法 都没有用,data.icol(0)   #取data的第一列,,data.irow(0)取第一行  
        for i in cc:
            cl.append(i)
        dft.columns = cl
        dft = dft.drop(0)
        dft.fillna(0.0)
        dft.to_csv(hbpath+code+'.csv',index = False)
        print code + ' is done'
        '''


if __name__ == '__main__':
    '''
    测试用
    '''
    hbzycb()