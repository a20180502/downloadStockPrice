import pymysql
from yahoo_finance import Share

conn = pymysql.connect(host='localhost', user='yahoo', password='yahoopassword', db='yahoodb', autocommit=True)
cursor = conn.cursor()

TABLENAME = 'yahooStock'
company_list = ['YHOO', 'AAOI', 'KEM', 'EVRI', 'GOOGL', 'AMZN', 'FB', 'AAPL']
START_DATE = '2017-05-07'
END_DATE = '2017-05-10'

#Create a new table to save data
def createTable():
    dropTable = 'DROP TABLE IF EXISTS `' + TABLENAME + '`;'
    cursor.execute(dropTable)
    sqlCreateTable = 'CREATE TABLE ' + TABLENAME +' (company_id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,' \
                     'Volume INT UNSIGNED NOT NULL, Symbol VARCHAR(30) NOT NULL, Adj_Close FLOAT UNSIGNED NOT NULL, ' \
                     'High FLOAT UNSIGNED NOT NULL, Low FLOAT UNSIGNED NOT NULL, DATE DATE NOT NULL, Close FLOAT UNSIGNED NOT NULL,' \
                     'Open FLOAT UNSIGNED NOT NULL);'
    cursor.execute(sqlCreateTable)

#Select all data from table
def selectAll():
    sqlSelect = 'SELECT * FROM ' + TABLENAME + ';'
    numRows = cursor.execute(sqlSelect)
    for row in range(numRows):
        print cursor.fetchone()

#Insert data into table
def insertRow(l):
    sqlInsert = 'INSERT INTO ' + TABLENAME + ' VALUE (NULL'
    for key in l.keys():
        sqlInsert += ', \'' + l[key] + '\''
    sqlInsert += ');'
    cursor.execute(sqlInsert)

createTable()
selectAll()
#loop through all companies
for company in company_list:
    comp = Share(company)
    allHistory = comp.get_historical(START_DATE, END_DATE)
    for l in allHistory:
        # print l
        insertRow(l)
selectAll()

cursor.close()
conn.close()

