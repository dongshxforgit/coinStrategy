# encoding=utf8

import time
import os
import csv
import ConfigParser 
import exceptions
import datetime
import copy

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#读取配置文件
filename = "./config/coinindex.ini"
coinCsvDirPath = "../../csvpath/"
coinindexfile = 'coinindexcept.csv'

class CoinParse(object):
    exchages = []
    exchangesVolumes = []
    lastExchangesVolumes = []
    instmtNames = []
    unChangeWaitTime = 0
    handleDataWaitTime = 0
    rows = []
    lastReadCoinTimes = []
    startJudgeCoinWaitTimes = []

    #load config 
    def loadConfig(self, config_file_path):
        try:
            cf = ConfigParser.ConfigParser()  
            cf.read(config_file_path) 

            exchageStr = cf.get("config", "exchanges")
            self.exchages = exchageStr.split(',')
            exchangesVolumeStr = cf.get("config", "exchangesVolumes")
            exchangesVolumes = exchangesVolumeStr.split(',')
            instmtNameStr = cf.get("config", "instmtNames")
            self.instmtNames = instmtNameStr.split(',')
            self.unChangeWaitTime = cf.getint("config", "unChangeWaitTime")
            self.handleDataWaitTime = cf.getint("config", "handleDataWaitTime")

            if len(self.exchages) <= 0 or len(self.instmtNames) <= 0:
                return -1

            for num in range(0, len(self.instmtNames)):
                self.exchangesVolumes.append(copy.deepcopy(exchangesVolumes))

            self.lastExchangesVolumes = copy.deepcopy(self.exchangesVolumes)

            if (os.path.exists(coinindexfile) == False):#不存在文件，则进行创建和数据添加
                with open(coinindexfile,"w") as csvfile: 
                    writer = csv.writer(csvfile)
                    rowIndex = copy.deepcopy(self.instmtNames)
                    rowIndex.append('update_time')
                    writer.writerow(rowIndex)


            return 0

        except Exception as error:
            print 'loadConfig err: ' + error
            return -1

    def handleAll(self):
        errNum = self.loadConfig(filename)
        if errNum != 0:
            print 'load config error, return'
            return
        print self.instmtNames

        allExangeCoinIndex = []
        while True:
            rowCoinIndex = []
            for instmtNameNum in range(0, len(self.instmtNames)):#获取需要读取的支付方式
                allExchageVolumes = 0 #交易所交易量总值
                exchangeCoinIndex = [] #单个交易所指数
                lastReadCoinTime = []
                startJudgeCoinWaitTime = []
                for num in range(0, len(self.exchages)):
                    filepath = coinCsvDirPath + 'exch_' + self.exchages[num] + '_' + self.instmtNames[instmtNameNum] + '_snapshot_' + time.strftime('%Y%m%d',time.localtime(time.time())) + '.csv'
                    coinFirstPriceForUsd,orderDateTime = self.handleOne(filepath)
                    if coinFirstPriceForUsd == 0:#说明还没有数据，将此时的交易所交易量设置为空
                        print self.exchangesVolumes[instmtNameNum][num]
                        self.exchangesVolumes[instmtNameNum][num] = 0
                    if (len(exchangeCoinIndex) < num + 1):
                        exchangeCoinIndex.append(copy.deepcopy(coinFirstPriceForUsd))
                    else:
                        exchangeCoinIndex[num] = copy.deepcopy(coinFirstPriceForUsd)
                    if len(self.lastReadCoinTimes) < (instmtNameNum + 1):    #判断是否第一次使用，第一次则填充数据
                        print len(self.lastReadCoinTimes)
                        lastReadCoinTime.append(copy.deepcopy(orderDateTime))
                        startJudgeCoinWaitTime.append(0)
                    elif self.lastReadCoinTimes[instmtNameNum][num] == orderDateTime:#此时表示数据未更新，进行时间长度判断
                        if self.startJudgeCoinWaitTimes[instmtNameNum][num] == 0:#表示第一次发现数据未更新
                            self.startJudgeCoinWaitTimes[instmtNameNum][num] = int(time.time())#超出未更新时间限制，此时将交易所交易量置为0
                        else :
                            if (int(time.time()) - self.startJudgeCoinWaitTimes[instmtNameNum][num] > self.unChangeWaitTime):
                                self.exchangesVolumes[instmtNameNum][num] = 0
                    else: #此时发现数据发生变化,将数据进行初始化
                        self.startJudgeCoinWaitTimes[instmtNameNum][num] = 0 
                        self.lastReadCoinTimes[instmtNameNum][num] = copy.deepcopy(orderDateTime)
                        self.exchangesVolumes[instmtNameNum][num] = copy.deepcopy(self.lastExchangesVolumes[instmtNameNum][num])
                    
                    allExchageVolumes += int (self.exchangesVolumes[instmtNameNum][num])
                    print  self.exchages[num] + ' ' + self.instmtNames[instmtNameNum] + ' ',coinFirstPriceForUsd

                self.lastReadCoinTimes.append(copy.deepcopy(lastReadCoinTime))
                self.startJudgeCoinWaitTimes.append(copy.deepcopy(startJudgeCoinWaitTime))
                #遍历完某一种货币所有交易所的获取到的指数后，最后获取最后的指数
                nowCoinIndex = 0
                if (allExchageVolumes == 0):
                    nowCoinIndex = 0
                else:
                    for num in range(0, len(self.exchages)):
                        nowCoinIndex += (float(self.exchangesVolumes[instmtNameNum][num]) / float(allExchageVolumes)) * exchangeCoinIndex[num]

                print self.instmtNames[instmtNameNum] + ' coinindex: ', nowCoinIndex
                rowCoinIndex.append(copy.deepcopy(nowCoinIndex))
            
            rowCoinIndex.append(copy.deepcopy(datetime.datetime.now().strftime("%Y%m%d %H:%M:%S.") + str(datetime.datetime.now().microsecond)))

            #将数据存放到csv文件中
            with open(coinindexfile,"a") as csvfile: 
                writer = csv.writer(csvfile)

                #先写入columns_name
                writer.writerow(rowCoinIndex)   
            time.sleep(self.handleDataWaitTime/1000)
                


    def handleOne(self, coin_file_path):
        try:
            with open(coin_file_path) as f:
                rows = f.readlines()
                targetLineup = rows[0]
                targetLine = rows[-1]
                a=targetLine.split(',')
            #获取常量b1 b2 b3 b4 b5 a1 a2 a3 a4 a5 bq1 bq2 bq3 bq3 bq4 bq5 aq1 aq2 aq3 aq4 aq5 order_data_time
            #默认对应列表为 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23
            allThreshold = 0
            listThreshold = []
            coinFirstPriceForUsd = 0
            orderDateTime = ''
            if len(a) > 24 and a[0] != 'id':#表示数据正常
                orderDateTime = a[23]
                for num in range(13, 23):
		    if (float(a[num]) > 50):
			allThreshold += 50
		        a[num] = '50'
		    else:
                    	allThreshold += float(a[num])

                for num in range(13, 23):
                    coinFirstPriceForUsd += (float(a[num - 10]) * float(a[num]) / allThreshold)
            return coinFirstPriceForUsd, orderDateTime
        except Exception as error:
            print error
	    return 0,''


coinParse = CoinParse()

def main():
    coinParse.handleAll()


if __name__ == '__main__':
    main()
