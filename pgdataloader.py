import random
import os
import psycopg2
from psycopg2 import OperationalError
from faker import Faker
import base64
import maskpass
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
fake = Faker()

#gluserer = input('Enter Username = ')
#glpasswd = maskpass.askpass('Enter Password = ')
#glhost = input('Enter host = ')
#glport = input('Enter port = ')

gluserer = 'postgres'
glpasswd = 'amokachi'
glhost = '10.0.0.133'
glport = '5432'

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
    newdbname = input('What do you want to call it?: ')
    print (newdbname)
    conn = psycopg2.connect(database='postgres', 
                            user=gluserer, 
                            password=glpasswd, 
                            host=glhost, 
                            port= glport)
    try:
      print('Connected to DB...')
      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(newdbname)))
      conn.commit()
      conn.close() 
      print('Creating Schema... ')
      conn = psycopg2.connect(database=newdbname, 
                              user=gluserer, 
                              password=glpasswd, 
                              host=glhost, 
                              port=glport)

      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      cursor.execute(f"CREATE SCHEMA {newdbname}")
      conn.commit()
      conn.close() 

      conn = psycopg2.connect(database=newdbname, 
                            user=gluserer, 
                            password=glpasswd, 
                            host=glhost, 
                            port=glport)
      print('Creating Tables...')
      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      create_table_query = f""" 
          CREATE TABLE {newdbname}.reporting (
            orderid uuid NOT NULL,
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
            unitcost money,
            orderdate timestamp without time zone,
            duration numeric(5,0),
            agentname character varying(30),
            contacttype character varying(20) COLLATE pg_catalog."default",
            CONSTRAINT reporting_pkey PRIMARY KEY (orderid)
        )
        """
      cursor.execute(create_table_query)
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
    dbforloading = input('Which db: ')
    try:
      conn = psycopg2.connect(
      database=dbforloading, user=gluserer, password=glpasswd, host=glhost, port=glport
      )
      conn.autocommit = True

      recs = input('Records to import = ')
      conrecs = int(recs)

      #Creating a cursor object using the cursor() method
      cursor = conn.cursor()
      for x in range(conrecs):
          orderdate = (fake.date_time_between_dates(datetime_start='-1y'),)
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
          fakeagentname = (fake.word(ext_word_list=['Alice','Boban', 'Charlie', 'Dieter', 'Ernst','Floella','Gregorio', 'Flavia']))
          fakecontacttype = (fake.word(ext_word_list=[ 'Call - Inbound', 'Call - Outbound', 'E-Mail', 'KB Article', 'Webchat']))
          fakeduration = (random.randint(60, 3000))
          loaderscript = f"INSERT INTO {dbforloading}.reporting (orderdate,orderid,userid,customerfname,customerlname,customeremail,customerstate, orditem, orderqty,ordercolor,ordersize,ordernumber,unitcost,agentname,contacttype,duration)\
                      VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
          cursor.execute(loaderscript,(orderdate,fakeuid,userid,fakefname,fakelname,email,state,fakeitem,orderqty,fakecolor,fakesize,ordernum,fakeunitcost,fakeagentname,fakecontacttype,fakeduration))
      conn.commit()
      conn.close()   

      print (f"* {conrecs} Records Inserted *")
    except OperationalError as e:
        print(f"Error inserting records: {e}")
#####################################################################################
def trashdb():
  cls() 
  dropdb = input('Which DB do you want to drop? : ')
  print("Enter password below if you're super cereal about dropping the DB")
  glpasswdconf = maskpass.askpass(f"Re-Enter {gluserer} password  = ")
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
    cursor.execute(f"DROP DATABASE IF EXISTS {dropdb} WITH (FORCE)")    
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