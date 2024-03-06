import pyodbc 
import os
import random
import maskpass
import base64
from faker import Faker
fake = Faker()
def cls():
    os.system('cls' if os.name=='nt' else 'clear')


#SERVER = input('Server Address: ')
#DATABASE = input('DB Name: ')
#USERNAME = input('User Name: ')

cls()
PASSWORD = maskpass.askpass('Enter Password = ')
SERVER = '10.0.0.127\REPORTING,1434'
USERNAME = 'yellowfinadmin'

def maskpasswd():
  cls()
  mpwd = maskpass.askpass('Enter Password = ')
  mpwd2 = maskpass.askpass('Re-enter Password = ')
  if mpwd == mpwd2:
    print('passwords match')
    print(base64.b64encode(mpwd.encode("utf-8")))
  else:
    print ('No Match.')

##############################################
def createdb():
    cls()
    try:
        connstr = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={'master'};UID={USERNAME};PWD={PASSWORD}'
        conn = pyodbc.connect(connstr, autocommit=True)

        print('Connected to DB...')
        cursor = conn.cursor()    
        cursor.execute(f"CREATE DATABASE aa_sample_data")    
        print('Done')
        #for r in records:

        sqlquery=(""" 
            USE [aa_sample_data]
            CREATE TABLE [dbo].[sales](
                [uuid] [uniqueidentifier] NOT NULL,
                [userid] [varchar](50) NULL,
                [customerfname] [varchar](50) NULL,
                [customerlname] [varchar](50) NULL,
                [customeremail] [varchar](50) NULL,
                [customerstate] [varchar](50) NULL,
                [orderitem] [varchar](50) NULL,
                [orderqty] [numeric](18, 0) NULL,
                [ordercolor] [varchar](50) NULL,
                [ordersize] [varchar](50) NULL,
                [ordernumber] [varchar](50) NULL,
                [unitcost] [money] NULL,
                [orderdate] [datetime] NULL,
            CONSTRAINT [PK_sales] PRIMARY KEY CLUSTERED 
            (
                [uuid] ASC
            )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
            ) ON [PRIMARY]  
        """)
        cursor.execute(sqlquery) 

    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate) 
##################################################################
def loaddata():
    cls()
    try:        
        recs = input('Records to import = ')
        conrecs = int(recs)
        connstr = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={'aa_sample_data'};UID={USERNAME};PWD={PASSWORD}'
        conn = pyodbc.connect(connstr, autocommit=True)
            #Creating a cursor object using the cursor() method
        cursor = conn.cursor()
        for x in range(conrecs):
            orderdate = (fake.date_time_between_dates(datetime_start='-5y'))
            fakeuid = (fake.uuid4())
            userid = (fake.domain_word())
            fakefname = (fake.first_name())
            fakelname = (fake.last_name())
            email = (fake.free_email())
            state = (fake.state())
            ordernum = (fake.sbn9())
            fakeitem = (fake.word(ext_word_list=['Polo Shirt','Travel Mug', 'Umbrella', 'Sunglasses', 'Water Bottle','Socks']))
            orderqty = (random.randint(1, 10))
            fakecolor = (fake.safe_color_name())
            fakesize = (fake.word(ext_word_list=[ 'Small', 'Medium', 'Large', 'X-Large', 'Kids']))
            fakeunitcost = (fake.random_int(min=10, max=20))
            cursor.execute("insert into dbo.sales \
                (uuid,userid,customerfname,customerlname,customeremail,customerstate,orderitem,orderqty,ordercolor,ordersize,ordernumber,unitcost,orderdate)\
                values (?,?,?,?,?,?,?,?,?,?,?,?,?);", \
                fakeuid,userid,fakefname,fakelname,email,state,fakeitem,orderqty,fakecolor,fakesize,ordernum,fakeunitcost,orderdate)
        print(f'{recs} inserted') 
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate) 

##########################################################

def trashdb():
    cls() 
    print("Enter password below if you're super cereal about dropping the DB")
    glpasswdconf = maskpass.askpass(f"Re-Enter {USERNAME} password  = ")
    if glpasswdconf == PASSWORD:
        print('passwords match')
    #try:
        connstr = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={'master'};UID={USERNAME};PWD={PASSWORD}'
        conn = pyodbc.connect(connstr, autocommit=True)
        cursor = conn.cursor()
        cursor.execute('ALTER DATABASE aa_sample_data SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE aa_sample_data')
        print('SampleDB Successfully dropped.')
    else: 
        print('Wrong password, database not dropped')

ans=True
cls()
while ans:
    print ("""
    1.Mask a password for connecting
    2.Create sample Database
    3.Load records in to DB
    4.Trash database
    5.Quit
    """)
    ans=input("What would you like to do? ") 
    if ans=="1": 
      maskpasswd() 
    elif ans=="2":
      createdb()
    elif ans=="3":
      loaddata()
    elif ans=="4":
      trashdb()
    elif ans=="5":
      print("\n Goodbye") 
      break
    elif ans !="":
      print("\n Not Valid Choice Try again") 
