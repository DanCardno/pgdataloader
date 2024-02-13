import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

#uname = input('Enter Username = ')
#conpasswd = input('Enter Password (paste masked) = ')
#conhost = input('Enter Host = ')
#conport = input('Enter connection port = ')

conn = psycopg2.connect(database="postgres", 
                        user='postgres', 
                        password='YW1va2FjaGk=', 
                        host='10.0.0.129', 
                        port= '5432')

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()
cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier('aa_sample_db')))
conn.commit()
conn.close() 



conn = psycopg2.connect(database="aa_sample_db", 
                        user='postgres', 
                        password='amokachi', 
                        host='10.0.0.129', 
                        port= '5432')

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()
cursor.execute(
    """ CREATE SCHEMA IF NOT EXISTS sales AUTHORIZATION postgres;
    """ )
conn.commit()
conn.close() 




conn = psycopg2.connect(database="aa_sample_db", 
                        user='postgres', 
                        password='amokachi', 
                        host='10.0.0.129', 
                        port= '5432')

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