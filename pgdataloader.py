import random
import os
import psycopg2
from faker import Faker
import base64
import maskpass
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
fake = Faker()

gluserer = input('Enter Username = ')
glpasswd = maskpass.askpass('Enter Password = ')
glhost = input('Enter host = ')
glport = input('Enter port = ')

def cls():
    os.system('cls' if os.name=='nt' else 'clear')


#####################################################################################
def maskpasswd():
  mpwd = maskpass.askpass('Enter Password = ')
  mpwd2 = maskpass.askpass('Re-enter Password = ')
  if mpwd == mpwd2:
    print('passwords match')
    print(base64.b64encode(mpwd.encode("utf-8")))
  else:
    print ('No Match.')
# input("Press Enter to complete...")
#####################################################################################
def createdb():
    cls()
    conn = psycopg2.connect(database='postgres', 
                            user=gluserer, 
                            password=glpasswd, 
                            host=glhost, 
                            port= glport)
    try:
      print('Connected to DB...')
      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier('aa_sample_db')))
      conn.commit()
      conn.close() 
      print('Creating Schema... ')
      conn = psycopg2.connect(database="aa_sample_db", 
                              user=gluserer, 
                              password=glpasswd, 
                              host=glhost, 
                              port=glport)

      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      cursor.execute(
          """ CREATE SCHEMA IF NOT EXISTS sales AUTHORIZATION postgres;
          """ )
      conn.commit()
      conn.close() 

      conn = psycopg2.connect(database="aa_sample_db", 
                            user=gluserer, 
                            password=glpasswd, 
                            host=glhost, 
                            port=glport)
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
    cls()
    try:
      conn = psycopg2.connect(
      database="aa_sample_db", user=gluserer, password=glpasswd, host=glhost, port=glport
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

      print (f"***** {conrecs} Records Inserted ****")
    except:
       print('Error - Check DB exists')
#####################################################################################
def trashdb():
  cls() 

  print('Delete sample database? ')
  glpasswdconf = maskpass.askpass(f"Re-Enter {gluserer} password to confirm = ")
  if glpasswdconf == glpasswd:
    print('passwords match')
  #try:
    conn = psycopg2.connect(database="postgres", 
                            user=gluserer, 
                            password=glpasswdconf, 
                            host=glhost, 
                            port= glport)

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(sql.SQL("DROP DATABASE aa_sample_db WITH (FORCE);"))
    conn.commit()
    conn.close()
    print('SampleDB Successfully dropped.')
  else: 
  #except:
    print('Wrong password, database not dropped')

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