# -*- coding: utf-8 -*-
'''
#url = 'http://quotes.money.163.com/service/xjllb_000002.html'
#url = 'http://quotes.money.163.com/service/lrb_000002.html'
#url = 'http://quotes.money.163.com/service/zcfzb_000002.html'
#三大报表
url = 'http://quotes.money.163.com/service/zycwzb_000002.html?type=report'#主要财务指标
url = 'http://quotes.money.163.com/service/cwbbzy_000002.html'#财报摘要
url = 'http://quotes.money.163.com/service/zycwzb_000002.html?type=report&part=ylnl'#盈利能力
url = 'http://quotes.money.163.com/service/zycwzb_000002.html?type=report&part=chnl'#偿还能力
url = 'http://quotes.money.163.com/service/zycwzb_000002.html?type=report&part=cznl'#成长能力
url = 'http://quotes.money.163.com/service/zycwzb_000002.html?type=report&part=yynl'#营运能力
'''
import tushare as ts
import os , sys
import pandas as pd
from urllib import urlretrieve
import time
import datetime
from stockrun.standard import standard_stock as ss

class catch(object):
    '''
    确定路径，codelist[]抓取，全量抓取两种方式；
    save_k()与save_report()两种部分
    '''
    def __init__(self,path = '',codelist = []):
        if path == '':
            self.savepath = 'D:/py/resource/basicdata/'
        else:
            self.savepath = path
        if codelist == []:
            self.codelist = ts.get_stock_basics()
        else:
            self.codelist = pd.DataFrame()
            self.codelist['code'] = codelist

    def CodeStr(self,codepd):
        '''
        用于将Code变成6位数的代码
        '''
        codepd = codepd.reset_index()
        # 获取所有股票代码（以6开头的，应该是沪市数据）集合
        codeList = []
        codel = codepd['code']
        for item in codel:
            item = str(item)
            while 1:
                if len(item) < 6:
                    item = '0'+item
                if len(item) == 6:
                    codeList.append(item)
                    break
        return codeList

    def catchreport(self,cl,path):
        '''
        内部使用
        '''
        codelist = cl
        donelist = []
        notdonelist = []
        balancepath = path + 'balance/'
        cashflowpath = path + 'cashflow/'
        profitpath = path + 'profit/'
        zycwzbpath = path + 'zycwzb/'
        ylnlpath = path + 'ylnl/'
        cwbbzypath = path + 'cwbbzy/'
        chnlpath = path + 'chnl/'
        cznlpath = path + 'cznl/'
        yynlpath = path + 'yynl/'
        ss.savepath(balancepath)
        ss.savepath(cashflowpath)
        ss.savepath(profitpath)
        ss.savepath(zycwzbpath)
        ss.savepath(ylnlpath)
        ss.savepath(cwbbzypath)
        ss.savepath(chnlpath)
        ss.savepath(cznlpath)
        ss.savepath(yynlpath)

        i = 10
        for code in codelist:
            try:
                cashflowurl = 'http://quotes.money.163.com/service/xjllb_'+code+'.html'
                profiturl = 'http://quotes.money.163.com/service/lrb_'+code+'.html'
                balanceurl = 'http://quotes.money.163.com/service/zcfzb_'+code+'.html'
                zycwzburl = 'http://quotes.money.163.com/service/zycwzb_'+code+'.html?type=report'#主要财务指标
                cwbbzyurl = 'http://quotes.money.163.com/service/cwbbzy_'+code+'.html'#财报摘要
                ylnlurl = 'http://quotes.money.163.com/service/zycwzb_'+code+'.html?type=report&part=ylnl'#盈利能力
                chnlurl = 'http://quotes.money.163.com/service/zycwzb_'+code+'.html?type=report&part=chnl'#偿还能力
                cznlurl = 'http://quotes.money.163.com/service/zycwzb_'+code+'.html?type=report&part=cznl'#成长能力
                yynlurl = 'http://quotes.money.163.com/service/zycwzb_'+code+'.html?type=report&part=yynl'#营运能力
                urlretrieve(cashflowurl, cashflowpath + code + '.csv')
                urlretrieve(profiturl, profitpath + code + '.csv')
                urlretrieve(balanceurl, balancepath + code + '.csv')
                urlretrieve(zycwzburl, zycwzbpath + code + '.csv')
                urlretrieve(cwbbzyurl, cwbbzypath + code + '.csv')
                urlretrieve(ylnlurl, ylnlpath + code + '.csv')
                urlretrieve(chnlurl, chnlpath + code + '.csv')
                urlretrieve(cznlurl, cznlpath + code + '.csv')
                urlretrieve(yynlurl, yynlpath + code + '.csv')
                donelist.append(code)
            except:
                print(str(code) + ' is not done')
                i = i - 1
                if i == 0:
                    notdonelist = list(set(codelist).difference(set(donelist)))
                    #查出不同，有点高级
                    print(u'本次完成%d只股票报告抓取'%(len(donelist)))
                    return notdonelist
            else:
                print(code + '  is done,no.%d has done')%len(donelist)
        notdonelist = list(set(codelist).difference(set(donelist)))
        print(u'本次完成%d只股票报告抓取'%(len(donelist)))
        return notdonelist
    
    def save_report(self,retrytime = 5000):
        '''
        要求东西保存在：D:/py/result/notdone/notdonereport里面
        必须要先存在文件夹：D:/py/result/notdone
        可以设定retrytime次数，默认5000次，够用了。
        '''
        if(os.path.exists('D:/py/result/notdone/notdonereport.csv')):
            codepd = pd.read_csv('D:/py/result/notdone/notdonereport.csv')
            print( u'继续上次未完成')
        else:
            codepd = self.codelist
        filepath = self.savepath
        codelist = self.CodeStr(codepd)
        i = retrytime          #记录次数，过多次数就放弃，存下剩余
        while 1:
            notdonelist = self.catchreport(codelist,filepath)
            if notdonelist == []:
                print(u'完成收工')
                dirPath = "d:/py/result/notdone"
                print( u'移除前notdone目录下的文件：%s' %os.listdir(dirPath))
                if(os.path.exists(dirPath+"/notdonereport.csv")):
                    os.remove(dirPath+"/notdonereport.csv")
                    print(u'移除后notdone目录下有文件：%s' %os.listdir(dirPath))
                else:
                    print(u"要删除的文件不存在！")
                break
            elif i == 0:
                savepd = pd.DataFrame()
                savepd['code'] = notdonelist 
                savepd.to_csv('d:/py/result/notdone/notdonereport.csv',encoding = 'GB18030')
                print(u'剩余code保存在d:/py/result/notdone/notdonereport中')
                break
            else:
                i = i - 1
                print(u'%d 只股票还没有完成'%len(notdonelist))
                print(u'还有%d 次机会，let us sleep 11.5 min'%i)
                savepd = pd.DataFrame()
                savepd['code'] = notdonelist 
                savepd.to_csv('D:/py/result/notdone/notdonereport.csv',encoding = 'GB18030')
                print(datetime.datetime.now().time())
                time.sleep(300)
                codelist = notdonelist
                continue
        return 0
                 
        
if __name__ == '__main__':
    '''
    测试用
    '''
    a = catch(path='D:/py/stockdata/')
    a.save_report()