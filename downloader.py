import pymysql
from yahoo_finance import Share

conn = pymysql.connect(host='localhost', user='yahoo', password='yahoopassword', db='yahoodb')
cursor = conn.cursor()

TABLENAME = 'record'
company_list = ['YHOO']
START_DATE = '2017-05-07'
END_DATE = '2017-05-10'

def createTable():
    dropTable = 'DROP TABLE IF EXISTS `' + TABLENAME + '`;'
    cursor.execute(dropTable)
    sqlCreateTable = 'CREATE TABLE record (company_id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,' \
                     'Volume VARCHAR(30) NOT NULL, Symbol VARCHAR(30) NOT NULL, Adj_Close VARCHAR(30) NOT NULL, ' \
                     'High VARCHAR(30) NOT NULL, Low VARCHAR(30) NOT NULL, DATE VARCHAR(30) NOT NULL, Close VARCHAR(30) NOT NULL,' \
                     'Open VARCHAR(30) NOT NULL);'
    cursor.execute(sqlCreateTable)

def selectAll():
    sqlSelect = 'SELECT * FROM ' + TABLENAME + ';'
    numRows = cursor.execute(sqlSelect)
    for row in range(numRows):
        print cursor.fetchone()

def insertRow(l):
    sqlInsert = 'INSERT INTO ' + TABLENAME + ' VALUE (NULL'
    for key in l.keys():
        sqlInsert += ', \'' + l[key] + '\''
    sqlInsert += ');'
    cursor.execute(sqlInsert)

createTable()
selectAll()
for company in company_list:
    comp = Share(company)
    allHistory = comp.get_historical(START_DATE, END_DATE)
    for l in allHistory:
        # print l
        insertRow(l)
selectAll()

cursor.close()
conn.close()


# sqlInsert = 'INSERT INTO stock VALUE'
#
#
# for n in l:
#     print n['Volume']
#
# conn = pymysql.connect(host='localhost', user='yahoo', password='yahoopassword', db='yahoodb')
#
# a = conn.cursor()

# sql = 'SELECT * from `stock`;'
# a.execute(sql)
#
# countrow = a.execute(sql)
# print ("Number of row :", countrow)
#
# data = a.fetchall()
# print(data)