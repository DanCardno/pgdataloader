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
glhost = (input('Enter host = ') or "localhost")
glport = (input('Enter port (default 5432) = ') or "5432")
gldbname = "(No DB Loaded)"



#####################################################################################

def cls():
    os.system('cls' if os.name=='nt' else 'clear')

#####################################################################################
def maskpasswd():
  cls()
  mpwd = maskpass.askpass('Enter Password = ')
  mpwd2 = maskpass.askpass('Re-enter Password = ')
  if mpwd == mpwd2:
    print('\npasswords match')
    encoded_password = base64.b64encode(mpwd.encode("utf-8"))
    formatted_result = encoded_password.decode("utf-8")

    print(f"\nEncoded password: {formatted_result}")
  else:
    print ('No Match.')
  input("\nPress Enter to complete...")
#####################################################################################
# DB Creation
# database
# - schema
# - tables


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

          TABLESPACE pg_default;

          ALTER TABLE IF EXISTS {newdbname}.agents
              OWNER to postgres;
          -- Index: idx_agents_agentid

          -- DROP INDEX IF EXISTS {newdbname}.idx_agents_agentid;

          CREATE INDEX IF NOT EXISTS idx_agents_agentid
              ON {newdbname}.agents USING btree
              (agent_id ASC NULLS LAST)
              TABLESPACE pg_default;
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
              CONSTRAINT customers_pkey PRIMARY KEY (customerid)
          )

          TABLESPACE pg_default;

          ALTER TABLE IF EXISTS {newdbname}.customers
              OWNER to postgres;
          -- Index: idx_customers_id

          -- DROP INDEX IF EXISTS {newdbname}.idx_customers_id;

          CREATE INDEX IF NOT EXISTS idx_customers_id
              ON {newdbname}.customers USING btree
              (customerid ASC NULLS LAST)
              TABLESPACE pg_default;
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
                orderid uuid NOT NULL,
                customerid uuid,
                orderdate timestamp without time zone,
                orderitem uuid,
                qty numeric(4,0),
                size character varying(15) COLLATE pg_catalog."default",
                agent uuid,
                orderstage character varying(20) COLLATE pg_catalog."default",
                CONSTRAINT orders_pkey1 PRIMARY KEY (orderid),
                CONSTRAINT fk_customer_id FOREIGN KEY (customerid)
                    REFERENCES {newdbname}.customers (customerid) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS {newdbname}.orders
                OWNER to postgres;
            -- Index: idx_orders_agent

            -- DROP INDEX IF EXISTS {newdbname}.idx_orders_agent;

            CREATE INDEX IF NOT EXISTS idx_orders_agent
                ON {newdbname}.orders USING btree
                (agent ASC NULLS LAST)
                TABLESPACE pg_default;
            -- Index: idx_orders_customer

            -- DROP INDEX IF EXISTS {newdbname}.idx_orders_customer;

            CREATE INDEX IF NOT EXISTS idx_orders_customer
                ON {newdbname}.orders USING btree
                (customerid ASC NULLS LAST)
                TABLESPACE pg_default;
            -- Index: idx_orders_id

            -- DROP INDEX IF EXISTS {newdbname}.idx_orders_id;

            CREATE INDEX IF NOT EXISTS idx_orders_id
                ON {newdbname}.orders USING btree
                (orderid ASC NULLS LAST)
                TABLESPACE pg_default;
            """
        cursor.execute(create_orders_query)
        conn.commit()
        conn.close() 
        print('Orders Created')
    except psycopg2.Error as e:
        print(f"Error creating database: {e}")


    ######### CALLS TABLE #########
    conn = psycopg2.connect(database=newdbname, 
                                  user=gluserer, 
                                  password=glpasswd, 
                                  host=glhost, 
                                  port=glport)
    try:
      print('Creating Calls Table...')
      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      create_calls_query = f""" 
            CREATE TABLE IF NOT EXISTS {newdbname}.calls
                (
                    callref uuid NOT NULL,
                    agent uuid,
                    customer uuid,
                    date timestamp without time zone,
                    reason character varying(30) COLLATE pg_catalog."default",
                    duration integer,
                    reasoncode character varying(30) COLLATE pg_catalog."default",
                    sentiment character varying(30) COLLATE pg_catalog."default",
                    resolved boolean,
                    answertime integer,
                    calldirection character varying(11) COLLATE pg_catalog."default",
                    CONSTRAINT calls_pkey PRIMARY KEY (callref)
                )

                TABLESPACE pg_default;

                ALTER TABLE IF EXISTS {newdbname}.calls
                    OWNER to postgres;
                -- Index: idx_calls_agent

                -- DROP INDEX IF EXISTS {newdbname}.idx_calls_agent;

                CREATE INDEX IF NOT EXISTS idx_calls_agent
                    ON {newdbname}.calls USING btree
                    (agent ASC NULLS LAST)
                    TABLESPACE pg_default;
                -- Index: idx_calls_callref

                -- DROP INDEX IF EXISTS {newdbname}.idx_calls_callref;

                CREATE INDEX IF NOT EXISTS idx_calls_callref
                    ON {newdbname}.calls USING btree
                    (callref ASC NULLS LAST)
                    TABLESPACE pg_default;
            """
      cursor.execute(create_calls_query)
      conn.commit()
      conn.close() 
      print('Calls Created')

    except psycopg2.Error as e:
      print(f"Error creating database: {e}")

    ######### ITEM TABLE #########
    conn = psycopg2.connect(database=newdbname, 
                                  user=gluserer, 
                                  password=glpasswd, 
                                  host=glhost, 
                                  port=glport)
    try:
      print('Creating Stock Item Table...')
      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      create_item_query = f""" 
            CREATE TABLE IF NOT EXISTS {newdbname}.items
              (
                  item_id SERIAL PRIMARY KEY, unique_id uuid DEFAULT gen_random_uuid(),
                  name character varying(100) COLLATE pg_catalog."default" NOT NULL,
                  color character varying(50) COLLATE pg_catalog."default",
                  price numeric(10,2) NOT NULL,
                  description text COLLATE pg_catalog."default",
                  category character varying(50) COLLATE pg_catalog."default",
                  stock_quantity integer DEFAULT 0,
                  created_at timestamp without time zone DEFAULT now(),
                  updated_at timestamp without time zone DEFAULT now()
              )

              TABLESPACE pg_default;

              ALTER TABLE IF EXISTS {newdbname}.items
                  OWNER to postgres;
              -- Index: idx_items_id

              -- DROP INDEX IF EXISTS {newdbname}.idx_items_id;

              CREATE INDEX IF NOT EXISTS idx_items_id
                  ON {newdbname}.items USING btree
                  (unique_id ASC NULLS LAST)
                  TABLESPACE pg_default;
            """
      cursor.execute(create_item_query)
      conn.commit()
      conn.close() 
      print('Stock Items Created')
      input("Done, press Enter to continue...")
    except psycopg2.Error as e:
      print(f"Error creating database: {e}")
      input("Done, press Enter to continue...")

      global gldbname
      gldbname = newdbname

#####################################################################################
def loaddata():
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
        print(f"Selected database: {selected_database}")
        dbforloading = selected_database
    else:
        print("Invalid selection")
    
    try:
      conn = psycopg2.connect(
      database=dbforloading, user=gluserer, password=glpasswd, host=glhost, port=glport
      )
      conn.autocommit = True

      #recs = input('Records to import = ')
      #conrecs = int(recs)

      #Creating a cursor object using the cursor() method
      cursor = conn.cursor()
      for x in range(20):
          geo=(fake.local_latlng(country_code='US', coords_only=True))
          
          fakeuid = (fake.uuid4())
          fakefname = (fake.first_name())
          fakelname = (fake.last_name())
          email = (fake.free_email())
          state = (fake.word(ext_word_list=[ 'Houston - HQ', 'Denver', 'Cleveland', 'Oakland', 'Nashville']))
          fakelatitude = (geo[0])
          fakelongitude = (geo[1])
          
          agentinsertscript  = f"INSERT INTO {dbforloading}.agents (agent_id, afname, alastname, aemail, alocation) VALUES (%s,%s,%s,%s,%s)"
          cursor.execute(agentinsertscript,(fakeuid,fakefname,fakelname,email,state))
      conn.commit()
      conn.close()   
      print (f"* Agents added *")

      conn = psycopg2.connect(
      database=dbforloading, user=gluserer, password=glpasswd, host=glhost, port=glport
      )
      conn.autocommit = True
      cursor = conn.cursor()
      for x in range(500):
          geo=(fake.local_latlng(country_code='US', coords_only=True))
          
          fakeuid = (fake.uuid4())
          userid = (fake.domain_word())
          fakefname = (fake.first_name())
          fakelname = (fake.last_name())
          email = (fake.free_email())
          state = (fake.state())
          fakelatitude = (geo[0])
          fakelongitude = (geo[1])
          customerinsertscript = f"INSERT INTO {dbforloading}.customers (customerid, cfname, clname, cemail, caddress, cstate,lat,long) \
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
          cursor.execute(customerinsertscript,(fakeuid,fakefname,fakelname,email,state,state,fakelatitude,fakelongitude))
      conn.commit()
      conn.close()   
      print (f"* Customers added *")

      conn = psycopg2.connect(
      database=dbforloading, user=gluserer, password=glpasswd, host=glhost, port=glport
      )
      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cursor = conn.cursor()
      create_item_query = f"""             
            INSERT INTO  {dbforloading}.items (name, color, price, description, category, stock_quantity) VALUES
            ('Red Chair', 'Red', 49.99, 'A comfortable red chair', 'Furniture', 10),
            ('Blue Desk', 'Blue', 89.99, 'A sturdy blue desk', 'Furniture', 5),
            ('Green Lamp', 'Green', 19.99, 'A stylish green lamp', 'Lighting', 20),
            ('Yellow Pillow', 'Yellow', 9.99, 'A soft yellow pillow', 'Bedding', 50),
            ('Black Sofa', 'Black', 299.99, 'A modern black sofa', 'Furniture', 2),
            ('White Table', 'White', 159.99, 'A sleek white table', 'Furniture', 8),
            ('Pink Mug', 'Pink', 5.99, 'A cute pink mug', 'Kitchenware', 100),
            ('Orange Blanket', 'Orange', 24.99, 'A warm orange blanket', 'Bedding', 25),
            ('Gray Shelf', 'Gray', 69.99, 'A durable gray shelf', 'Storage', 15),
            ('Purple Rug', 'Purple', 39.99, 'A soft purple rug', 'Decor', 12);
            """
      cursor.execute(create_item_query)
      conn.commit()
      conn.close() 
      print('* Stock Items Added *')



      input("Press Enter to complete...")
    except OperationalError as e:
        print(f"Error inserting records: {e}")
#####################################################################################

# Order and calls info
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
      recs = input('Records to add = ')
      conrecs = int(recs)    
      cursor = conn.cursor()
      for x in range(conrecs): 
        ordernum = (fake.uuid4())
        orderdate = (fake.date_time_between_dates(datetime_start='-5d'),)

#### Select item to insert ####
        fakeitem = f"""select unique_id from {dbforloading}.items order by random() limit 1""" 
        cursor.execute(fakeitem)
        fakeitem=cursor.fetchone()
##################################################
##        
        
#        fakecolor = (fake.safe_color_name())
#        fakesize = (fake.word(ext_word_list=[ 'Small', 'Medium', 'Large', 'X-Large', 'Kids']))
#        fakeunitcost = (fake.random_int(min=10, max=20))
#### Pull a single customer for the order data ####
        selcustomer = f"""select customerid from {dbforloading}.customers order by random() limit 1""" 
        cursor.execute(selcustomer)
        customerselect=cursor.fetchone()[0]
###################################################

#### Pull a single agent for the order data ####
        saleagent = f"""select agent_id from {dbforloading}.agents order by random() limit 1""" 
        cursor.execute(saleagent)
        agentselect=cursor.fetchone()[0]
###################################################
        orderqty = (random.randint(1, 10))
        orderstage = (fake.word(ext_word_list=[ 'Picking', 'Awaiting Pickup', 'Quality Check', 'Packing', 'Shipped', 'Delivered', 'Returned' ]))
        orderdatascript = f"INSERT INTO {dbforloading}.orders (orderid, customerid, orderdate, orderitem, qty, agent, orderstage) \
            VALUES (%s,%s,%s,%s,%s,%s,%s)"   
        cursor.execute(orderdatascript,(ordernum,customerselect,orderdate,fakeitem,orderqty,agentselect,orderstage))
      conn.commit()
      conn.close()   
      print (f"* {conrecs} Records Inserted *")
    except OperationalError as e:
        print(f"Error inserting records: {e}")


################# CALL DATA #############################

    try:
      conn = psycopg2.connect(database=dbforloading, user=gluserer, password=glpasswd, host=glhost, port=glport)
      conn.autocommit = True
#      recs = input('Records to import = ')
      conrecs = int(recs)    
    #Creating a cursor object using the cursor() method
      cursor = conn.cursor()
      for x in range(conrecs): 
        callref = (fake.uuid4())
        selectscript = f"""select agent_id from {dbforloading}.agents order by random() limit 1"""
        cursor.execute(selectscript)
        callagent=cursor.fetchone()[0]
        calldate = (fake.date_time_between_dates(datetime_start='-5d'),)
#### Pull a single customer for the call data ####
        cusselectscript = f"""select customerid from {dbforloading}.customers order by random() limit 1"""
        cursor.execute(cusselectscript)
        callcustomer=cursor.fetchone()[0]
####################################################
        th = 80 / 100
        calldirection = "Inbound" if random.random() < th else "Outbound"
        callreason = (fake.word(ext_word_list=[ 'Billing Enquiry', 'Complaint', 'Bill Payment', 'Technical Support', 'Other']))
        callreasoncode = (fake.word(ext_word_list=[ 'Research', 'Documentation', 'Manager Escalation', 'Break', 'Transfer']))
        callduration = (fake.random_int(min=50, max=500))
        callresolved = random.choice([True, False])
        callanswertime = (fake.random_int(min=10, max=20))
        callsentiment = (fake.word(ext_word_list=[ 'Happy', 'Frustrated', 'Angry', 'Distressed', 'Satisfied' ,'Dissapointed' ,'Relieved','Betrayed','Joyful']))
        orderdatascript = f"INSERT INTO {dbforloading}.calls (callref, agent, customer, date, reason, duration, resolved, answertime, reasoncode,calldirection,sentiment) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"   
        cursor.execute(orderdatascript,(callref,callagent,callcustomer,calldate,callreason,callduration,callresolved,callanswertime,callreasoncode,calldirection,callsentiment))
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
    #except
    print('Wrong password, database not dropped')
    input(f"\n{glpasswd}Press Enter to complete...")

#####################################################################################

ans=True
while ans:
    cls()
    print ("""
    1.Mask a password for connecting
    2.Create sample database
    3.Load agent / customer data
    4.Load order call / data
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
