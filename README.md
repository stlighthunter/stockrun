# stockrun
A股数据分析
主要用于A股数据分析，数据来源163，有部分数据来自tushare
1.将数据存放在mysql中，包括财报，k线，主要有以下数据库：
kline，balance，profit，cashflow，report
每个数据库中有以股票代码命名的数据tables
2.按要求对以上数据进行update
3.有基本计算模型：
PETTM，PB等计算后存放在同命名的数据库，数据按code命名
4.有财务表头对照表，放在O数据库中，tablde_name = ‘cbbt’
5.planB中主要照顾没有条件mysql的，存放在本地的csv
6.所有存放在mysql中数据，都会有个文件夹用来存放元文件。
7.有一个分析模型，目前思考中，慢慢更新。
