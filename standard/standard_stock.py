# -*- coding: utf-8 -*-
'''
此模块标准化Code等供相关模块使用。
StCode:将code编程标准数字
savepath:将文件路径保存，可以输入文件路径
codelist：返回标准code的list
codebasic：返回DataFrame格式的code_basic文件
'''
import tushare as ts
import pandas as pd
import tushare as ts
import os

def StCode(code):
    '''
    将int或者str变成标准化的股票代码
    '''
    item = str(code)
    notnumber = list(set(item).difference(set('1234567890')))
    if len(notnumber) >= 1:
        print '请按照股票代码填写'
        return 0
    if len(item) < 6:
        item = item.zfill(6)
        return item
    elif len(item) == 6:
        return item
    elif len(item) > 6:
        print '输入位数多过6位'
        return 0

def savepath(path):
    '''
    确定地址是否存在，如不存在，就创建
    可以输入文件地址
    '''
    filepath = os.path.split(path)[0]
    if os.path.exists(filepath):
        return 0
    else:
        os.makedirs(filepath)
        return 0 
    
def codelist(filepath = 'D:/py/result/allcode.csv',save = 1):
    '''
    给于一个标准化的股票代码list
    '''
    if(os.path.exists(filepath)):
        codepd = pd.read_csv(filepath,dtype={'code':str},encoding='GB18030')
        #读取本地数据，并code读取为str格式
        codepd['code'] = codepd['code'].str.zfill(6)
        #将code格式全部变成6位数的str
    else:
        try:
            codepd = ts.get_stock_basics()
            codepd = codepd.reset_index()
            # 获取所有股票代码集合
        except:
            print 'retry later'
            return 0
        savepath(filepath)
        codepd.to_csv(filepath,index = False,encoding = "GB18030") 
        codepd = pd.read_csv(filepath,dtype={'code':str},encoding='GB18030')
        #读取本地数据，并code读取为str格式
    codeList = []
    codel = codepd['code']
    for code in codel:
        codeList.append(code)
    if save != 1:
        os.remove(filepath)    
    return codeList    

def codebasic(filepath = 'D:/py/result/allcode.csv',save = 1):
    '''
    给一个标准化的dataframe的基本面数据
    '''
    if(os.path.exists(filepath)):
        codebasic = pd.read_csv(filepath,dtype={'code':str},encoding='GB18030')
        #读取本地数据，并code读取为str格式
    else:
        try:
            codepd = ts.get_stock_basics()
            codebasic = codepd.reset_index()
            # 获取所有股票代码
            savepath(filepath)
        except:
            print 'retry later'
            return 0
    codebasic['code'] = codebasic['code'].str.zfill(6)
    #将code格式全部变成6位数的str
    if save == 1:
        codebasic.to_csv(filepath,encoding="GB18030")
    return codebasic



if __name__ == '__main__':
    print StCode(07)
    
    '''
    import matplotlib.pyplot as plt
    xlist = []
    colors = []
    x = float(360)/24
    for i in range(0,24):
        xlist.append(x)
        colors.append('red') 
    plt.pie(xlist,colors=colors)
    plt.show()
    '''