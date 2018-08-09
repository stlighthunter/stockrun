# -*- coding: utf-8 -*-
'''
此模块用于建立文件存储路径，同时在tushare上面抓取三大表格，K线数据等
用catch()模块,中有save_k()，save_report(),save_pe()，update_k()
save_pettm()五个模块
quotes.money.163.com模块中，不设置fields反而能多获得‘成交笔数’数据
'''
import tushare as ts
import os , sys
import shutil 
import pandas as pd
import urllib
import time
import datetime
from stockrun.data import count_stock as cs
from stockrun.standard import standard_stock as ss
class catch(object):
    '''
    确定路径，codelist[]抓取，全量抓取两种方式；
    save_k()与save_report(),save_pe()部分
    '''
    def __init__(self,path = '',codelist = []):
        if path == '':
            self.savepath = 'd:/py/stockdata/'
        else:
            self.savepath = path
        if codelist == []:
            self.codelist = ss.codebasic()
        else:
            self.codelist = pd.DataFrame()
            self.codelist['code'] = codelist
   
    def catchstock(self,cl,filepath,start = '0'):
        '''
        内部使用
        '''
        codeList = cl  #list格式
        donelist = []
        notdonelist = []
        filepath = filepath
        ss.savepath(filepath)
        nowyear = datetime.date.today().year
        i = 5
        for code in codeList:
            code = str(code)
            try:
                if code[0] == '6':
                    print '正在获取股票%s数据,第%d个。' % (str(code),len(donelist)+1)
                    url = 'http://quotes.money.163.com/service/chddata.html?code=0' + code + \
                    '&start='+str(start)+'&end='+str(nowyear)+'1231&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
                    urllib.urlretrieve(url, filepath + code + '.csv')
                    donelist.append(code)
                else:
                    print '正在获取股票%s数据,第%d个。' % (str(code),len(donelist)+1)
                    url = 'http://quotes.money.163.com/service/chddata.html?code=1' + code + \
                    '&start='+str(start)+'&end='+str(nowyear)+'1231&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
                    urllib.urlretrieve(url, filepath + code + '.csv')
                    donelist.append(code)
            except:
                i = i-1
                if i == 0:
                    break
        notdonelist = list(set(codeList).difference(set(donelist)))#查出不同，有点高级
        return notdonelist

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
        i = 5
        for code in codelist:
            try:
                cashflow = ts.get_cash_flow(code)
                balance = ts.get_balance_sheet(code)
                profit = ts.get_profit_statement(code)
                donelist.append(code)
            except:
                print str(code) + ' is not done'
                i = i - 1
                if i == 0:
                    notdonelist = list(set(codelist).difference(set(donelist)))
                    #查出不同，有点高级
                    print u'本次完成%d只股票报告抓取'%(len(donelist))
                    return notdonelist
            else:
                balance.to_csv(balancepath + code+'.csv',encoding = 'GB18030')
                profit.to_csv(profitpath + code + '.csv',encoding='GB18030')
                cashflow.to_csv(cashflowpath + code + '.csv',encoding='GB18030')
                print code + '  is done'
        notdonelist = list(set(codelist).difference(set(donelist)))
        print u'本次完成%d只股票报告抓取'%(len(donelist))
        return notdonelist
    
    def save_k(self):
        '''
        将K线数据保存在/stockdata/kline/文件夹中，
        一定保证这个文件夹存在，本程序没有检测代码，懒得写
        K线数据来自163的财经数据抓取，准不准不知道。        
        '''
        #Url = 'http://quote.eastmoney.com/stocklist.html'  # 163数据连接地址,目前方案使用self.codelist
        filepath = self.savepath + 'kline/'  # 定义数据文件保存路径
        # 实施抓取
        codepd = self.codelist         #不更改原来数据
        codeList = ss.codelist()
        while 1:
            notdonelist = self.catchstock(codeList,filepath)
            if notdonelist == []:
                break
            else:
                print '%d 只股票还没有完成'%len(notdonelist)
                print 'let us sleep 3 min'
                time.sleep(180)
                codeList = notdonelist
                continue
        print 'all done'
        return 0

    def save_report(self,retrytime = 5000):
        '''
        要求东西保存在：d:/py/result/notdone/notdonereport里面
        必须要先存在文件夹：d:/py/result/notdone
        可以设定retrytime次数，默认5000次，够用了。
        '''
        if(os.path.exists('d:/py/result/notdone/notdonereport.csv')):
            codepd = pd.read_csv('d:/py/result/notdone/notdonereport.csv')
            print u'继续上次未完成'
        else:
            codepd = self.codelist
        filepath = self.savepath
        codelist = ss.codelist()
        i = retrytime          #记录次数，过多次数就放弃，存下剩余
        while 1:
            notdonelist = self.catchreport(codelist,filepath)
            if notdonelist == []:
                print u'完成收工'
                dirPath = "d:/py/result/notdone"
                print u'移除前notdone目录下的文件：%s' %os.listdir(dirPath)
                if(os.path.exists(dirPath+"/notdonereport.csv")):
                    os.remove(dirPath+"/notdonereport.csv")
                    print u'移除后notdone目录下有文件：%s' %os.listdir(dirPath)
                else:
                    print u"要删除的文件不存在！"
                break
            elif i == 0:
                savepd = pd.DataFrame()
                savepd['code'] = notdonelist 
                savepd.to_csv('d:/py/result/notdone/notdonereport.csv',encoding = 'GB18030')
                print u'剩余code保存在d:/py/result/notdone/notdonereport中'
                break
            else:
                i = i - 1
                print u'%d 只股票还没有完成'%len(notdonelist)
                print u'还有%d 次机会，let us sleep 11.5 min'%i
                savepd = pd.DataFrame()
                savepd['code'] = notdonelist 
                savepd.to_csv('d:/py/result/notdone/notdonereport.csv',encoding = 'GB18030')
                print datetime.datetime.now().time()
                time.sleep(690)
                codelist = notdonelist
                continue
        return 0
    
    def save_pettm(self):
        '''
        计算出PETTM，并且保存在D:\py\result\PETTM中，
        所有的代码会生成一个allcode，D:/py/result/allcode.csv
        '''
        codel = ss.codelist()
        i = 1
        for code in codel:
            number = ss.StCode(code)
            print '第%d个:'%i
            i = i+1
            try:
                df = cs.PETTM(number,printif = 0,start='2001')
            except:
                print 'it is wrong'

    def update_k(self,notdonelist = []):
        '''
        每次只下载K线中没有的数据，需要用到d:/py/text/文件夹过渡
        报告过一次小错误，问题不大
        '''   
        filepath = 'd:/py/text/'
        ss.savepath(filepath)
        klinepath = self.savepath + 'kline/'
        if notdonelist == []:
            nextdo = os.listdir(klinepath)
        else:
            nextdo = notdonelist
        for code in nextdo:
            codename = code.split('.')[0]
            old = pd.read_csv(klinepath+code,encoding='GB18030')
            olddate = old[u'日期'][0]
            startdate = datetime.datetime.strptime(olddate,"%Y-%m-%d")+datetime.timedelta(days=1)
            #将datetime格式加1天
            startday = startdate.strftime("%Y%m%d")
            #将格式转换为str的yymmdd
            codelist = [codename]
            try:
                notdone = self.catchstock(codelist,filepath = filepath,start = startday)
                update = pd.read_csv(filepath+code,encoding='GB18030')
                new = update.append(old)
                new = new.reset_index()
                del new['index']
                new.to_csv(klinepath+code,encoding='GB18030',index=False)
                notdonelist = notdonelist + notdone
            except:
                print codename+'not done'
        shutil.rmtree(filepath) 
        #情况文件夹内文件 
        if notdonelist == []:
            print 'all done'
        else:
            print u'再次获取%d个未完成的股票。'%(len(notdonelist))
            self.update_k(notdonelist = notdonelist)

    def save_dpzs(self,start = '19900101'):
        nowyear = str(datetime.date.today().year)
        filepath = self.savepath + 'hgsj/dpzs/'
        if os.path.exists(filepath):
            #确定文件夹是否存在，如存在，就更新。不存在，重新下载。
            old = pd.read_csv(filepath+'szzs.csv',encoding='GB18030')
            olddate = old[u'日期'][0]
            startdate = datetime.datetime.strptime(olddate,"%Y-%m-%d")+datetime.timedelta(days=1)
            #将datetime格式加1天
            start = startdate.strftime("%Y%m%d")
            textfilepath = self.savepath + 'text/'
            ss.savepath(textfilepath)
        else:    
            ss.savepath(filepath)
            textfilepath = filepath
        for zs in ['0000001.szzs','1399001.szcz','1399006.cybz']:
            zsdm = zs.split('.')[0]
            zswjm = zs.split('.')[1]
            url = 'http://quotes.money.163.com/service/chddata.html?code='+ zsdm + \
                '&start='+start+'&end='+nowyear+'1231&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER'
            urllib.urlretrieve(url,textfilepath+zswjm+'.csv')
            if textfilepath != filepath:
                print u'更新大盘数据'
                old = pd.read_csv(filepath+zswjm+'.csv',encoding='GB18030')
                update = pd.read_csv(textfilepath+zswjm+'.csv',encoding='GB18030')
                new = update.append(old)
                new = new.reset_index()
                del new['index']
                new.to_csv(filepath+zswjm+'.csv',encoding='GB18030',index=False)
        return 0



if __name__ == '__main__':
    '''
    测试用
    '''
    a = catch()
    #a.save_k()
    #a.save_report()
    #a.save_pettm()
    a.update_k()
    #a.save_dpzs()
    #df = ts.get_profit_data()
    #df = ts.get_cashflow_data(2018,1)
    #print df
    #dn = ts.get_report_data(2018,1)
    #print dn
    #dr = ts.get_growth_data(2018,1)
    #print dr
