import json 
import pandas as pd
from kafka import KafkaConsumer
import pyodbc 
import asyncio
import time
import string


#pd.DataFrame( columns = ['InvoiceNo', 'StockCode', 'Description','Quantity','InvoiceDate','UnitPrice','CustomerID','Country'])
#df=pd.DataFrame([],columns=['string'])

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};Server=LAPTOP-689KD47T;Database=Online Retail;UID=thuande;PWD=thuande')
cursor = cnxn.cursor()

def consumkafka():
    consumer = KafkaConsumer(
        'Invoice',
        #bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest'
    )
    table = 'Invoice'
    cursor.execute("DROP TABLE INVOICE")
    cursor.execute("CREATE TABLE [dbo].["+table+"]([InvoiceNo] [nvarchar](10) NULL, [StockCode] [nvarchar](10) NULL, [Description] [nvarchar](80) NULL, [Quantity] [int] NULL, [InvoiceDate] [datetime] NULL, [UnitPrice] [decimal](18, 0) NULL, [CustomerID] [nvarchar](10) NULL, [Country] [nvarchar](20) NULL)")
    for message in consumer:
        strmess= json.loads(message.value)
        
        txtsql = strmess.replace('.|.',',')
        txtsql1 = "INSERT INTO INVOICE(InvoiceNo, StockCode, Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country) VALUES ("
        #print(txtsql1+txtsql +")")
        cursor.execute(txtsql1 +txtsql +")")
        print(txtsql)
        cnxn.commit()
       

#development idea
async def savesql():
    time.sleep(300)
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};Server=LAPTOP-689KD47T;Database=Online Retail;UID=thuande;PWD=thuande')
    cursor = cnxn.cursor()
    #cursor.execute("INSERT INTO INVOICE(InvoiceNo, StockCode, Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country) VALUES ('536365AA','85123A','WHITE HANGING HEART T-LIGHT HOLDER',6,'01-12-2010 08:26:00',2.55,'17850','United Kingdom')")  
    while True:
        txtsql1 = "'INSERT INTO INVOICE(InvoiceNo, StockCode, Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country) VALUES ("
        
        #cursor.execute(txtsql1 +txtsql +")')")



if __name__ == '__main__':
    # Kafka Consumer 
    
    consumkafka()