from tkinter import *
import tkinter.messagebox
import random
import string
import time 
import json 
import random 
#import pandas as pd
from datetime import datetime, timedelta
import subprocess
import sys


from kafka import KafkaProducer


# Messages will be serialized as JSON 
def serializer(message):
    return json.dumps(message).encode('utf-8')

def sendkafka(message):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=serializer
    )        
            # Send it to our 'messages' topic
    print(f'Producing message @ {datetime.now()} | Message = {message}')
    producer.send('Invoice', message)
    

def validate():
    # Kafka Producer
    
    InvoiceNo= "'"+entry_1.get() +"'" 
    StockCode= "'"+entry_2.get() +"'" 
    Description= "'"+entry_3.get()+"'" 
    Quantity= entry_4.get()
    InvoiceDate= "'"+entry_5.get()+"'" 
    UnitPrice= entry_6.get()
    CustomerID= "'"+entry_7.get()+"'" 
    Country= "'"+entry_8.get()+"'" 
    message= ".|.".join([InvoiceNo, StockCode, Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country])
    if (InvoiceNo=="" or  StockCode=="" or  Description=="" or Quantity=="" or InvoiceDate=="" or UnitPrice=="" or CustomerID=="" or Country ==""):
        tkinter.messagebox.showinfo('Invalid Message Alert',"Fields cannot be left empty!")

    else:
        sendkafka(message)
        tkinter.messagebox.showinfo('Success Message',"Send Invoice")
        

def runConsumer():
    subprocess.Popen(["start", "cmd", "/k", "python consumer.py"], shell = True)
    tkinter.messagebox.showinfo('Success Message',"Chay Consumer.py")
        
def generate():
    clear()
    entry_1.insert(0,''.join(random.choices(string.digits, k=7) + random.choices(string.ascii_uppercase,k=1)))
    entry_2.insert(0,''.join(random.choices(string.digits, k=5) + random.choices(string.ascii_uppercase,k=1)))
    entry_3.insert(0,''.join(random.choices(string.ascii_lowercase , k=16)))
    entry_4.insert(0,random.randint(1,100))
    entry_5.insert(0,str(random.randrange(2010, 2014))+'-'+str((random.randrange(1, 12)))+'-'+ str((random.randrange(1, 28)))+' '+'00:00:00')
    entry_6.insert(0,round(random.uniform(20, 60),3))
    entry_7.insert(0,''.join(random.choices(string.digits, k=5) + random.choices(string.ascii_uppercase,k=1)))
    entry_8.insert(0,''.join(random.choices(string.ascii_uppercase, k=15)))

def clear():
    entry_1.delete(0,END)
    entry_2.delete(0,END)
    entry_3.delete(0,END)
    entry_4.delete(0,END)
    entry_5.delete(0,END)
    entry_6.delete(0,END)
    entry_7.delete(0,END)
    entry_8.delete(0,END)
   
def now():
    entry_5.delete(0,END)
    entry_5.insert(0,datetime.now().replace(microsecond=0))

root = Tk()
root.geometry('600x500')
root.title("Registration Form")




label_0 = Label(root, text="Registration form",width=20,font=("bold", 20))
label_0.place(x=90,y=23)
Button(root, text='Generate',width=15,bg='blue',fg='white', command = generate).place(x=300,y=80)
Button(root, text='Run Consumer.py',width=15,bg='blue',fg='white', command = runConsumer).place(x=120,y=80)
label_1 = Label(root, text="InvoiceNo",width=20,font=("bold", 10))
label_1.place(x=80,y=130)

entry_1 = Entry(root)
entry_1.place(x=240,y=130)

label_2 = Label(root, text="StockCode",width=20,font=("bold", 10))
label_2.place(x=68,y=160)

entry_2 = Entry(root)
entry_2.place(x=240,y=160)

label_3 = Label(root, text="Description",width=20,font=("bold", 10))
label_3.place(x=70,y=190)
entry_3 = Entry(root)
entry_3.place(x=240,y=190)

label_4 = Label(root, text="Quantity",width=20,font=("bold", 10))
label_4.place(x=70,y=220)
entry_4 = Entry(root)
entry_4.place(x=240,y=220)


label_5 = Label(root, text="InvoiceDate",width=20,font=("bold", 10))
label_5.place(x=85,y=250)
entry_5 = Entry(root)
entry_5.place(x=240,y=250)
Button(root, text='Now',width=5,bg='white',fg='Black',borderwidth=2, command = now).place(x=400,y=250)

label_6 = Label(root, text="UnitPrice",width=20,font=("bold", 10))
label_6.place(x=85,y=280)

entry_6 = Entry(root)
entry_6.place(x=240,y=280)

label_7 = Label(root, text="CustomerID",width=20,font=("bold", 10))
label_7.place(x=85,y=310)

entry_7 = Entry(root)
entry_7.place(x=240,y=310)

label_8 = Label(root, text="Country",width=20,font=("bold", 10))
label_8.place(x=85,y=340)

entry_8 = Entry(root)
entry_8.place(x=240,y=340)


Button(root, text='Submit',width=20,bg='green',fg='white', command = validate).place(x=180,y=380)

root.mainloop()
