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

gluserer = (input('Enter Username (default postgres) = ') or "postgres")
glpasswd = maskpass.askpass('Enter Password = ')
glhost = (input('Enter host = ') or "10.0.0.133")
glport = (input('Enter port (default 5432) = ') or "5432")
gldbname = "(No DB Loaded)"

#####################################################################################

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
    #print (newdbname)
    conn = psycopg2.connect(database='postgres', 
                            user=gluserer, 
                            password=glpasswd, 
                            host=glhost, 
                            port= glport)
    try:
        print('Connected to DB...')
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE {newdbname};")
        print(f"Database '{newdbname}' created successfully")
    except psycopg2.Error as e:
        print(f"Error creating database: {e}")

    ######### SCHEMA #########
    print('Creating Schema... ')
    conn = psycopg2.connect(database=newdbname, 
                            user=gluserer, 
                            password=glpasswd, 
                            host=glhost, 
                            port=glport)
    try:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute(f"CREATE SCHEMA {newdbname}")
        conn.commit()
        conn.close() 
    except psycopg2.Error as e:
        print(f"Error creating database: {e}")
   
    ######### AGENT TABLE #########
    conn = psycopg2.connect(database=newdbname, 
                            user=gluserer, 
                            password=glpasswd, 
                            host=glhost, 
                            port=glport)
    try:
      print('Creating Agent Table...')
      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      create_agents_query = f""" 
        CREATE TABLE IF NOT EXISTS {newdbname}.agents
        (
            agent_id uuid NOT NULL,
            afname character varying(20) COLLATE pg_catalog."default",
            alastname character varying(20) COLLATE pg_catalog."default",
            aemail character varying(60) COLLATE pg_catalog."default",
            alocation character varying(60) COLLATE pg_catalog."default",
            CONSTRAINT agents_pkey PRIMARY KEY (agent_id)
        )
        """
      cursor.execute(create_agents_query)
      conn.commit()
      conn.close() 
      print('Agents Created')
    except psycopg2.Error as e:
        print(f"Error creating database: {e}")
    
    ######### CUSTOMER TABLE #########
    conn = psycopg2.connect(database=newdbname, 
                            user=gluserer, 
                            password=glpasswd, 
                            host=glhost, 
                            port=glport)
    try:
    
      print('Creating Customer Table...')
      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      create_customers_query = f""" 
        CREATE TABLE IF NOT EXISTS {newdbname}.customers
        (
            customerid uuid NOT NULL,
            cfname character varying(20) COLLATE pg_catalog."default",
            clname character varying(20) COLLATE pg_catalog."default",
            cemail character varying(60) COLLATE pg_catalog."default",
            caddress character varying(60) COLLATE pg_catalog."default",
            cstate character varying(60) COLLATE pg_catalog."default",
            lat double precision,
            "long" double precision,
            CONSTRAINT orders_pkey PRIMARY KEY (customerid)
        )
        """
      cursor.execute(create_customers_query)
      conn.commit()
      conn.close() 
      print('Agents Created')
    except psycopg2.Error as e:
        print(f"Error creating database: {e}")

     ######### ORDERS TABLE #########
    conn = psycopg2.connect(database=newdbname, 
                            user=gluserer, 
                            password=glpasswd, 
                            host=glhost, 
                            port=glport)
    try:
    
        print('Creating Order Table...')
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        create_orders_query = f""" 
            CREATE TABLE IF NOT EXISTS {newdbname}.orders
            (
                orderid character varying(20) COLLATE pg_catalog."default" NOT NULL,
                customerid uuid,
                orderdate timestamp without time zone,
                orderitem character varying COLLATE pg_catalog."default",
                qty numeric(4,0),
                color character varying(15) COLLATE pg_catalog."default",
                unitprice numeric(6,0),
                size character varying(15) COLLATE pg_catalog."default",
                CONSTRAINT orders_pkey1 PRIMARY KEY (orderid),
                CONSTRAINT fk_customer_id FOREIGN KEY (customerid)
                    REFERENCES {newdbname}.customers (customerid) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
            )
            """
        cursor.execute(create_orders_query)
        conn.commit()
        conn.close() 
        print('Orders Created')
        input("Done, press Enter to continue...")
    except psycopg2.Error as e:
        print(f"Error creating database: {e}")

        global gldbname
        gldbname = newdbname



#####################################################################################
def loaddata():
    cls()
    cur = conn.cursor()
    cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
    rows = cur.fetchall()
    numbered_databases = {}
    for i, row in enumerate(rows):
        numbered_databases[i + 1] = row[0]
        print(f"{i + 1}. {row[0]}")

    selection = int(input("Select a database by entering its number: "))
    selected_database = numbered_databases.get(selection)

    if selected_database:
        print(f"Selected database: {selected_database}")
        dbforloading = selected_database
    else:
        print("Invalid selection")
    
    
    
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
          geo=(fake.local_latlng(country_code='US', coords_only=True))
          
          fakeuid = (fake.uuid4())
          userid = (fake.domain_word())
          fakefname = (fake.first_name())
          fakelname = (fake.last_name())
          email = (fake.free_email())
          state = (fake.state())
#          fakeagentname = (fake.word(ext_word_list=['Alice','Boban', 'Charlie', 'Dieter', 'Ernst','Floella','Gregorio', 'Flavia']))
#         fakecontacttype = (fake.word(ext_word_list=[ 'Call - Inbound', 'Call - Outbound', 'E-Mail', 'KB Article', 'Webchat', 'Virtual Session','Administration']))
#          fakeduration = (random.randint(60, 3000))
          fakelatitude = (geo[0])
          fakelongitude = (geo[1])
          
          agentinsertscript  = f"INSERT INTO {dbforloading}.agents (agent_id, afname, alastname, aemail, alocation) VALUES (%s,%s,%s,%s,%s)"
          customerinsertscript = f"INSERT INTO {dbforloading}.customers (customerid, cfname, clname, cemail, caddress, cstate,lat,long) \
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
         # loaderscript = f"INSERT INTO {dbforloading}.reporting (orderdate,orderid,userid,customerfname,customerlname,customeremail,customerstate, orditem, orderqty,ordercolor,ordersize,ordernumber,unitcost,agentname,contacttype,duration,latitude,longitude)\
         #             VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
         # cursor.execute(loaderscript,(orderdate,fakeuid,userid,fakefname,fakelname,email,state,fakeitem,orderqty,fakecolor,fakesize,ordernum,fakeunitcost,fakeagentname,fakecontacttype,fakeduration,fakelatitude,fakelongitude))
          cursor.execute(agentinsertscript,(fakeuid,fakefname,fakelname,email,state))
          cursor.execute(customerinsertscript,(fakeuid,fakefname,fakelname,email,state,state,fakelatitude,fakelongitude))
      conn.commit()
      conn.close()   
      print (f"* {conrecs} Records Inserted *")

      input("Press Enter to complete...")
    except OperationalError as e:
        print(f"Error inserting records: {e}")
#####################################################################################
def orderdata():
    cls()
    conn = psycopg2.connect(
      database='postgres', user=gluserer, password=glpasswd, host=glhost, port=glport
      )
    conn.autocommit = True
    
    cur = conn.cursor()
    cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
    rows = cur.fetchall()
    numbered_databases = {}
    for i, row in enumerate(rows):
        numbered_databases[i + 1] = row[0]
        print(f"{i + 1}. {row[0]}")

    selection = int(input("Select a database by entering its number: "))
    selected_database = numbered_databases.get(selection)

    if selected_database:
        cls()
        print(f"Selected database: {selected_database}")
        dbforloading = selected_database
    else:
        print("Invalid selection")
        orderdata()
        

    try:
      conn = psycopg2.connect(database=dbforloading, user=gluserer, password=glpasswd, host=glhost, port=glport)
      conn.autocommit = True
      recs = input('Records to import = ')
      conrecs = int(recs)    
    #Creating a cursor object using the cursor() method
      cursor = conn.cursor()
      for x in range(conrecs): 
        ordernum = (fake.sbn9())
        orderdate = (fake.date_time_between_dates(datetime_start='-1y'),)
        fakeitem = (fake.word(ext_word_list=['Polo Shirt','Travel Mug', 'Umbrella', 'Sunglasses', 'Water Bottle','Socks']))
        fakecolor = (fake.safe_color_name())
        fakesize = (fake.word(ext_word_list=[ 'Small', 'Medium', 'Large', 'X-Large', 'Kids']))
        fakeunitcost = (fake.random_int(min=10, max=20))
        selectscript = f"""select customerid from {dbforloading}.customers order by random() limit 1"""
        orderqty = (random.randint(1, 10))
        cursor.execute(selectscript)
        useruuid=cursor.fetchone()[0]
        orderdatascript = f"INSERT INTO {dbforloading}.orders (orderid, customerid, orderdate, orderitem,qty,color,size,unitprice) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"   
        cursor.execute(orderdatascript,(ordernum,useruuid,orderdate,fakeitem,orderqty,fakecolor,fakesize,fakeunitcost))
      conn.commit()
      conn.close()   
      print (f"* {conrecs} Records Inserted *")
      input("Press Enter to complete...")
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
    cls()
    print ("""
    1.Mask a password for connecting
    2.Create sample Database
    3.Load Agent / Customer data
    4.Load Order Data
    5.Trash DB
    6.Exit
    """)
    ans=input("What would you like to do? ") 
    if ans=="1": 
      maskpasswd() 
    elif ans=="2":
      createdb()
    elif ans=="3":
      loaddata()
    elif ans=="4":
      orderdata()
    elif ans=="5":
       trashdb()
    elif ans=="6":
      print("\n Goodbye") 
      break
    elif ans !="":
      print("\n Not Valid Choice Try again") 