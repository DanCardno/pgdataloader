import random
import psycopg2
from faker import Faker
import base64
import maskpass
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
fake = Faker()

#####################################################################################
def maskpasswd():
  mpwd = maskpass.askpass('Enter Password = ')
  mpwd2 = maskpass.askpass('Re-enter Password = ')
  if mpwd == mpwd2:
    print('passwords match')
    print(base64.b64encode(mpwd.encode("utf-8")))
  else:
    print (f'No Match.')
# input("Press Enter to complete...")
#####################################################################################
def createdb():

    uname = input('Enter Username = ')
    conpasswd = input('Enter Password (paste masked) = ')
    conhost = input('Enter Host = ')
    conport = input('Enter connection port = ')

    conn = psycopg2.connect(database='postgres', 
                            user=uname, 
                            password=conpasswd, 
                            host=conhost, 
                            port= conport)
    try:
      print('Connected to DB...')
      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier('aa_sample_db')))
      conn.commit()
      conn.close() 
      print('Creating Schema... ')
      conn = psycopg2.connect(database="aa_sample_db", 
                              user=uname, 
                              password=conpasswd, 
                              host=conhost, 
                              port= conport)

      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      cursor.execute(
          """ CREATE SCHEMA IF NOT EXISTS sales AUTHORIZATION postgres;
          """ )
      conn.commit()
      conn.close() 

      conn = psycopg2.connect(database="aa_sample_db", 
                            user=uname, 
                            password=conpasswd, 
                            host=conhost, 
                            port= conport)
      print('Creating Tables...')
      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      cursor.execute(
          """ CREATE TABLE IF NOT EXISTS sales.reporting (orderid uuid NOT NULL,
          userid character varying(30) COLLATE pg_catalog."default",
          customerfname character varying(30) COLLATE pg_catalog."default",
          customerlname character varying(30) COLLATE pg_catalog."default",
          customeremail character varying(50) COLLATE pg_catalog."default",
          customerstate character varying(30) COLLATE pg_catalog."default",
          orditem character varying(30) COLLATE pg_catalog."default",
          orderqty numeric(5,0),
          ordercolor character varying(15) COLLATE pg_catalog."default",
          ordersize character varying(10) COLLATE pg_catalog."default",
          ordernumber character varying(20) COLLATE pg_catalog."default",
          orderdate timestamp without time zone,
          CONSTRAINT reporting_pkey PRIMARY KEY (orderid)
          )
          TABLESPACE pg_default;
          """ )
      conn.commit()
      conn.close() 
    except:
      print('*** DB Creation Error ***')
      print('*** Check credentials / database already exists ***')
      print('*** Try using Trash database option... ***')

  
 #   input("Press Enter to complete...")

#####################################################################################
def loaddata():
    uname = input('Enter Username = ')
    conpasswd = input('Enter Password (paste masked) = ')
    conhost = input('Enter Host = ')
    conport = input('Enter connection port = ')
    conn = psycopg2.connect(
    database="aa_sample_db", user=uname, password=conpasswd, host=conhost, port= conport
    )
    conn.autocommit = True

    recs = input('Records to import = ')
    conrecs = int(recs)

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    for x in range(conrecs):
        orderdate = (fake.date_time_between_dates(datetime_start='-5y'),)
        fakeuid = (fake.uuid4())
        userid = (fake.domain_word())
        fakefname = (fake.first_name())
        fakelname = (fake.last_name())
        email = (fake.free_email())
        state = (fake.state())
        ordernum = (fake.sbn9())
        fakeitem = (fake.word(ext_word_list=['Polo Shirt','Travel Mug', 'Umbrella', 'Sunglasses']))
        orderqty = (random.randint(1, 10))
        fakecolor = (fake.safe_color_name())
        fakesize = (fake.word(ext_word_list=[ 'Small', 'Medium', 'Large', 'X-Large', 'Kids']))

        cursor.execute('''INSERT INTO sales.reporting(orderdate,orderid,userid,customerfname,customerlname,customeremail,customerstate, orditem, orderqty,ordercolor,ordersize,ordernumber)\
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',\
                    (orderdate,fakeuid,userid,fakefname,fakelname,email,state,fakeitem,orderqty,fakecolor,fakesize,ordernum))

    conn.commit()
    conn.close()   

    print (f"************** {conrecs} Records Inserted **************")

#####################################################################################
def trashdb():
    
    uname = input('Enter Username = ')
    conpasswd = input('Enter Password (paste masked) = ')
    conhost = input('Enter Host = ')
    conport = input('Enter connection port = ')
    conn = psycopg2.connect(database="postgres", 
                            user=uname, 
                            password=conpasswd, 
                            host=conhost, 
                            port= conport)

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(sql.SQL("DROP DATABASE aa_sample_db WITH (FORCE);"))
    conn.commit()
    conn.close() 

#####################################################################################
ans=True
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