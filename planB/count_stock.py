# -*- coding: utf-8 -*-
import pandas as pd
import tushare as ts
import time
import urllib
import datetime
import os
from stockrun.standard import standard_stock as ss

def obtain(code,filepath,update = 0):
    nowyear = datetime.date.today().year      #int格式
    now = str(nowyear)
    filepath = filepath
    if update == 0:
        if(os.path.exists(filepath + code + '.csv')):
            PeKline = pd.read_csv(filepath + code + '.csv', encoding="GB18030")
            print(u'已完成读取股票%s数据' % code)
            return PeKline
    try:
        if code[0] == '6':
            print(u'正在获取股票%s数据' % code)
            url = 'http://quotes.money.163.com/service/chddata.html?code=0' + code + \
                    '&end='+now+'1231&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
        else:
            print(u'正在获取股票%s数据' % code)
            url = 'http://quotes.money.163.com/service/chddata.html?code=1' + code + \
                    '&end='+now+'1231&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
        urllib.urlretrieve(url, filepath + code + '.csv')
        PeKline = pd.read_csv(filepath + code + '.csv', encoding="GB18030")
    except:
        print "wrong code number"
        PeKline = pd.DataFrame()
    return PeKline
    
def NowPE(code,filepath):
    '''
    用于计算当年的PE
    一般不单独使用，辅助配合PETTM
    默认输入code而已，计算此刻所在年份PETTM值
    '''
    klinepath = filepath+'kline/'
    profitpath = filepath+"profit/"
    PEk = obtain(code,klinepath)
    nowyear = datetime.date.today().year
    now = str(nowyear)
    if len(PEk) < 10:
        print "本年度数据过短"
        return 0
    dn = pd.read_csv(profitpath + code + ".csv", encoding="GB18030")
    dn1 = dn[dn[u'报表日期'] == u'归属于母公司所有者的净利润']  # 只要净利润那一行
    if len(dn1)<1:
        dn1 = dn[dn[u'报表日期'] == u'归属于母公司的净利润']

    n = 0
    lastyear = nowyear -1
    lastyearQ1 = str(lastyear) + '0331'  # 去年的年的财报
    lastyearQ2 = str(lastyear) + '0630'
    lastyearQ3 = str(lastyear) + '0930'
    lastyearQ4 = str(lastyear)+'1231'
    last2yearQ4 = str(lastyear-1)+'1231'
    last2yearQ3 = str(lastyear-1) + '0930'
    try:
        prolastyear = float(dn1[lastyearQ4])  #可以计算的基础利润
    except:
        proft1,proft2,proft3 = float(dn1[last2yearQ4]),float(dn1[last2yearQ3]),float(dn1[lastyearQ3])
        prolastyear = proft1-proft2+proft3

    nowyearQ1 = str(nowyear) + '0331'  # 现在所在年的财报
    nowyearQ2 = str(nowyear) + '0630'
    nowyearQ3 = str(nowyear) + '0930'
    list1 = dn.columns

    if nowyearQ1 in list1:
        Q1prof = float(dn1[nowyearQ1])  # 现在年的净利润
        Q1Lprof = float(dn1[lastyearQ1])
        n = 1
    if nowyearQ2 in list1:
        Q2prof = float(dn1[nowyearQ2])  # 现在年的净利润
        Q2Lprof = float(dn1[lastyearQ2])
        n = 2
    if nowyearQ3 in list1:
        Q3prof = float(dn1[nowyearQ3])  # 现在年的净利润
        Q3Lprof = float(dn1[lastyearQ3])
        n = 3

    zsz = PEk[PEk[u'日期'] > now]  # 建立今年的总市值数据

    nowyearQ1 = str(nowyear) + '-03-31'  # 现在所在年的K线数据，因为格式不同
    nowyearQ2 = str(nowyear) + '-06-30'
    nowyearQ3 = str(nowyear) + '-09-30'

    if n == 0:                                           #只有去年的季度数据
        prof = prolastyear                               #只有第一季度，用去年利润做为利润
        zszQ = zsz
        zszQ[u'pe'] = zszQ[u'总市值']/prof         #不用分割数据，全用去年数据
        zszQ = zszQ.sort_values(u'日期')
        return zszQ
    if n == 1:
        prof1 = prolastyear                               #存在本年度一季度报表
        zszQ1 = zsz[zsz[u'日期'] <= nowyearQ1]
        zszQ1[u'pe'] = zszQ1[u'总市值'].values / prof1

        zszQ2 = zsz[zsz[u'日期'] > nowyearQ1]
        zszQ2 = zsz[zsz[u'日期'] != nowyearQ1]

        prof2 = prolastyear - Q1Lprof + Q1prof
        zszQ2[u'pe']  = zszQ2[u'总市值'].values / prof2

        zszQ = zszQ1.append(zszQ2)
        zszQ.sort_values(u'日期')
        return zszQ
    if n == 2:
        prof1 = prolastyear                                 # 存在本年度一二季度报表
        zszQ1 = zsz[zsz[u'日期'] <= nowyearQ1]
        zszQ1[u'pe'] = zszQ1[u'总市值'].values / prof1

        zszQ = zsz[zsz[u'日期'] > nowyearQ1]
        zszQ = zszQ[zszQ[u'日期'] != nowyearQ1]

        zszQ2 = zszQ[zszQ[u'日期'] <= nowyearQ2]
        zszQ3 = zszQ[zszQ[u'日期'] > nowyearQ2]
        zszQ3 = zszQ3[zszQ3[u'日期'] != nowyearQ2]

        prof2 = prolastyear - Q1Lprof + Q1prof
        prof3 = prolastyear - Q2Lprof + Q2prof
        zszQ2[u'pe'] = zszQ2[u'总市值'].values / prof2
        zszQ3[u'pe'] = zszQ3[u'总市值'].values / prof3

        zszQ = zszQ1.append(zszQ2)
        zszQ = zszQ.append(zszQ3)
        zszQ.sort_values(u'日期')
        return zszQ
    if n==3:
        prof1 = prolastyear  # 存在本年度一二三季度报表
        zszQ1 = zsz[zsz[u'日期'] <= nowyearQ1]
        zszQ1[u'pe'] = zszQ1[u'总市值'].values / prof1

        zszQ = zsz[zsz[u'日期'] > nowyearQ1]
        zszQ = zszQ[zszQ[u'日期'] != nowyearQ1]
        zszQ = zszQ[zszQ[u'日期'] <= nowyearQ3]

        zszQ2 = zszQ[zszQ[u'日期'] <= nowyearQ2]
        zszQ3 = zszQ[zszQ[u'日期'] > nowyearQ2]
        zszQ3 = zszQ3[zszQ3[u'日期'] != nowyearQ2]
        zszQ4 = zsz[zsz[u'日期'] > nowyearQ3]
        zszQ4 = zszQ4[zszQ4[u'日期'] != nowyearQ3]

        prof2 = prolastyear - Q1Lprof + Q1prof
        prof3 = prolastyear - Q2Lprof + Q2prof
        prof4 = prolastyear - Q3Lprof + Q3prof
        zszQ2[u'pe'] = zszQ2[u'总市值'].values / prof2
        zszQ3[u'pe'] = zszQ3[u'总市值'].values / prof3
        zszQ4[u'pe'] = zszQ3[u'总市值'].values / prof4

        zszQ = zszQ1.append(zszQ2)
        zszQ = zszQ.append(zszQ3)
        zszQ = zszQ.append(zszQ4)
        zszQ.sort_values(u'日期')
        return zszQ

def PETTM(code,path = 'D:/py/stockdata/',printif = 1,update = 0,start = '2001'):
    '''
    计算PETTM，自动更新K线数据，存储路径可选，是否打印可选。
    默认：code为str格式，数据存在'D:\py\result\PETTM'，默认打印统计数据。
    '''
    code = ss.StCode(code)
    year= start
    nowyear = datetime.date.today().year      #int格式
    filepath = path
    klinepath = path + 'kline/'
    profitpath = path + "profit/" 
    PEk = obtain(code,klinepath,update=update)    
    nowclose = float(PEk[u'收盘价'][0])
    thisyear = str(nowyear)+'-01-01'
    if len(PEk[PEk[u'日期'] <= thisyear])==0:
        print 'this year stock'
        NowyearPe = NowPE(code,filepath)
        return NowyearPe
    if len(PEk) < 90:
        print u"day<90"
        return 0
    #lastyear = str(nowyear)+'-12-31'
    #PEk = PEk[PEk[u'日期'] <= lastyear] #避免计算两次当年数据，当年数据有NowPe计算
    try:
        dn = pd.read_csv(profitpath + code + ".csv", encoding="GB18030")
    except:
        return 0
    list1 = dn.columns
    n = int(year) -1             #确定头一年财报是否在
    while 1:
        date = (str(n) + '0331').decode('utf-8')
        if date in list1:
            start = n + 1
            break
        else:
            n = n + 1

    NewPe = pd.DataFrame()

    dn1 = dn[dn[u'报表日期'] == u'归属于母公司所有者的净利润']  # 只要净利润那一行
    if len(dn1) < 1:
        dn1 = dn[dn[u'报表日期'] == u'归属于母公司的净利润']

    for y in range(int(start) - 1, nowyear-1):  # 从2004年到2016年年报

        Q1 = str(y) + '0331'   #头一年的财报
        Q2 = str(y) + '0630'
        Q3 = str(y) + '0930'
        Q4 = str(y) + '1231'


        Q1prof = float(dn1[Q1])  # 头一年净利润
        Q2prof = float(dn1[Q2])  # 头一年净利润
        Q3prof = float(dn1[Q3])  # 头一年净利润
        Q4prof = float(dn1[Q4])  # 头一年净利润


        Q1N = str(y+1) + '0331'  #当年财报，只需要到Q3就可以
        Q2N = str(y+1) + '0630'
        Q3N = str(y+1) + '0930'
        Q1Nprof = float(dn1[Q1N])
        Q2Nprof = float(dn1[Q2N])
        Q3Nprof = float(dn1[Q3N])  #当年利润

        startyear = str(y + 1).encode("utf-8")
        endyear = str(y + 2).encode("utf-8")  # 做比较时：不包括上限,包括下限制，>2016,<2017表示2016年数据。


        zsz = PEk[PEk[u'日期'] < endyear]
        zsz = zsz[zsz[u'日期'] > startyear]  # 建立zsz一年的总市值数据

        zszQ1 = zsz[zsz[u'日期'] <= Q1N]
        zszQ4 = zsz[zsz[u'日期'] > Q3N]
        zszQ4 = zszQ4[zszQ4[u'日期'] != Q3N]

        zszQ = zsz[zsz[u'日期'] > Q1N]               #中间时间段
        zszQ = zszQ[zszQ[u'日期'] <= Q3N]

        zszQ2 = zszQ[zszQ[u'日期'] <= Q2N]
        zszQ3 = zszQ[zszQ[u'日期'] > Q2N]
        zszQ3 = zszQ3[zszQ3[u'日期'] != Q2N]

        profQ1 = Q4prof
        profQ2 = Q4prof-Q1prof+Q1Nprof
        profQ3 = Q4prof-Q2prof+Q2Nprof
        profQ4 = Q4prof-Q3prof+Q3Nprof

        zszQ1[u'pe'] = zszQ1[u'总市值'].values / profQ1
        zszQ2[u'pe'] = zszQ2[u'总市值'].values / profQ2
        zszQ3[u'pe'] = zszQ3[u'总市值'].values / profQ3
        zszQ4[u'pe'] = zszQ4[u'总市值'].values / profQ4

        PEsome = pd.DataFrame()
        PEsome = PEsome.append(zszQ1)
        PEsome = PEsome.append(zszQ2)
        PEsome = PEsome.append(zszQ3)
        PEsome = PEsome.append(zszQ4)
        PEsome = PEsome.sort_values(u'日期')
        NewPe = NewPe.append(PEsome)
    NowyearPe = NowPE(code,filepath)
    NewPe = NewPe.append(NowyearPe)
    NewPe = NewPe.sort_values(u'日期')
    
    NewPe.to_csv('D:/py/result/PETTM/' + code + '.csv', encoding="GB18030")
    if printif == 1:
        maxclose = nowclose*NewPe[u'pe'].max()/NewPe[u'pe'][0]
        minclose = nowclose*NewPe[u'pe'][NewPe[u'pe'] > 0].min()/NewPe[u'pe'][0]
        medclose = nowclose*NewPe[u'pe'].median()/NewPe[u'pe'][0]


        a = len(NewPe[NewPe[u'pe']<NewPe[u'pe'][0]])
        b = len(NewPe)
        c = float(a)/b
        name = NewPe[u'名称'][0]
        print name+u"目前PE为：%.2f"%(NewPe[u'pe'][0])
        print str(start)+u"年以来，目前PE超过%.2f%%的时候"%(c*100)
        print u'这段时间，最大PE：%.2f，最小PE：%.2f，中位数PE：%.2f' % (
        NewPe[u'pe'].max(), NewPe[u'pe'][NewPe[u'pe'] > 0].min(), NewPe[u'pe'].median())
        print u'按PE对应目前股价，最大：%.2f，最小：%.2f，中位数：%.2f' % (maxclose, minclose, medclose)
        print u'现在股票价格：%.2f'%nowclose
    return NewPe








if __name__ == '__main__':
    '''
    PETTM、NowPE可用。
    '''
    codepd = ss.codebasic()
    codepd = codepd.reset_index()
    codel = codepd['code']
    i = 1
    for code in codel:
        number = ss.StCode(code)
        print '第%d个:'%i
        i = i+1
        try:
            df = PETTM(number,printif = 0)
        except:
            print 'it is wrong'
    '''
    while 1:
        dn = ts.get_realtime_quotes('603799')
        dn['cha']= (float(dn['high'])-float(dn['price']))/float(dn['open'])*100
        print dn[['code','name','price','cha','high','low','date','time']]
        if datetime.datetime.now().time()>datetime.time(15, 00):
            break
        time.sleep(60)
    ''' 
