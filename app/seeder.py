import random
from faker import Faker
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from db import get_connection

fake = Faker()


def create_database(dbname: str, host=None, port=None, user=None, password=None):
    conn = get_connection(host=host, port=port, user=user, password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE {dbname};")
    conn.close()

    conn = get_connection(database=dbname, host=host, port=port, user=user, password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(f"CREATE SCHEMA {dbname}")
    conn.close()

    conn = get_connection(database=dbname, host=host, port=port, user=user, password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {dbname}.agents (
            agent_id uuid NOT NULL,
            afname character varying(20),
            alastname character varying(20),
            aemail character varying(60),
            alocation character varying(60),
            CONSTRAINT agents_pkey PRIMARY KEY (agent_id)
        );
        CREATE INDEX IF NOT EXISTS idx_agents_agentid
            ON {dbname}.agents USING btree (agent_id ASC NULLS LAST);
    """)
    conn.close()

    conn = get_connection(database=dbname, host=host, port=port, user=user, password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {dbname}.customers (
            customerid uuid NOT NULL,
            cfname character varying(20),
            clname character varying(20),
            cemail character varying(60),
            caddress character varying(60),
            cstate character varying(60),
            lat double precision,
            long double precision,
            CONSTRAINT customers_pkey PRIMARY KEY (customerid)
        );
        CREATE INDEX IF NOT EXISTS idx_customers_id
            ON {dbname}.customers USING btree (customerid ASC NULLS LAST);
    """)
    conn.close()

    conn = get_connection(database=dbname, host=host, port=port, user=user, password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {dbname}.orders (
            orderid uuid NOT NULL,
            customerid uuid,
            orderdate timestamp without time zone,
            orderitem uuid,
            qty numeric(4,0),
            size character varying(15),
            agent uuid,
            orderstage character varying(20),
            CONSTRAINT orders_pkey1 PRIMARY KEY (orderid),
            CONSTRAINT fk_customer_id FOREIGN KEY (customerid)
                REFERENCES {dbname}.customers (customerid)
                ON UPDATE NO ACTION ON DELETE NO ACTION
        );
        CREATE INDEX IF NOT EXISTS idx_orders_agent
            ON {dbname}.orders USING btree (agent ASC NULLS LAST);
        CREATE INDEX IF NOT EXISTS idx_orders_customer
            ON {dbname}.orders USING btree (customerid ASC NULLS LAST);
        CREATE INDEX IF NOT EXISTS idx_orders_id
            ON {dbname}.orders USING btree (orderid ASC NULLS LAST);
    """)
    conn.close()

    conn = get_connection(database=dbname, host=host, port=port, user=user, password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {dbname}.calls (
            callref uuid NOT NULL,
            agent uuid,
            customer uuid,
            date timestamp without time zone,
            reason character varying(30),
            duration integer,
            reasoncode character varying(30),
            sentiment character varying(30),
            resolved boolean,
            answertime integer,
            calldirection character varying(11),
            CONSTRAINT calls_pkey PRIMARY KEY (callref)
        );
        CREATE INDEX IF NOT EXISTS idx_calls_agent
            ON {dbname}.calls USING btree (agent ASC NULLS LAST);
        CREATE INDEX IF NOT EXISTS idx_calls_callref
            ON {dbname}.calls USING btree (callref ASC NULLS LAST);
    """)
    conn.close()

    conn = get_connection(database=dbname, host=host, port=port, user=user, password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {dbname}.items (
            item_id SERIAL PRIMARY KEY,
            unique_id uuid DEFAULT gen_random_uuid(),
            name character varying(100) NOT NULL,
            color character varying(50),
            price numeric(10,2) NOT NULL,
            description text,
            category character varying(50),
            stock_quantity integer DEFAULT 0,
            created_at timestamp without time zone DEFAULT now(),
            updated_at timestamp without time zone DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_items_id
            ON {dbname}.items USING btree (unique_id ASC NULLS LAST);
    """)
    conn.close()


def load_data(dbname: str, host=None, port=None, user=None, password=None):
    conn = get_connection(database=dbname, host=host, port=port, user=user, password=password)
    conn.autocommit = True
    cursor = conn.cursor()
    agents = []
    for _ in range(20):
        agents.append((
            fake.uuid4(),
            fake.first_name(),
            fake.last_name(),
            fake.free_email(),
            fake.word(ext_word_list=['Houston - HQ', 'Denver', 'Cleveland', 'Oakland', 'Nashville'])
        ))
    cursor.executemany(
        f"INSERT INTO {dbname}.agents (agent_id, afname, alastname, aemail, alocation) VALUES (%s,%s,%s,%s,%s)",
        agents
    )
    conn.close()

    conn = get_connection(database=dbname, host=host, port=port, user=user, password=password)
    conn.autocommit = True
    cursor = conn.cursor()
    customers = []
    for _ in range(500):
        geo = fake.local_latlng(country_code='US', coords_only=True)
        customers.append((
            fake.uuid4(),
            fake.first_name(),
            fake.last_name(),
            fake.free_email(),
            fake.state(),
            fake.state(),
            geo[0],
            geo[1]
        ))
    cursor.executemany(
        f"INSERT INTO {dbname}.customers (customerid, cfname, clname, cemail, caddress, cstate, lat, long) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        customers
    )
    conn.close()

    conn = get_connection(database=dbname, host=host, port=port, user=user, password=password)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"""
        INSERT INTO {dbname}.items (name, color, price, description, category, stock_quantity) VALUES
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
    """)
    conn.close()


def order_data(dbname: str, records: int, host=None, port=None, user=None, password=None):
    conn = get_connection(database=dbname, host=host, port=port, user=user, password=password)
    conn.autocommit = True
    cursor = conn.cursor()
    orders = []
    for _ in range(records):
        cursor.execute(f"SELECT unique_id FROM {dbname}.items ORDER BY random() LIMIT 1")
        item = cursor.fetchone()[0]
        cursor.execute(f"SELECT customerid FROM {dbname}.customers ORDER BY random() LIMIT 1")
        customer = cursor.fetchone()[0]
        cursor.execute(f"SELECT agent_id FROM {dbname}.agents ORDER BY random() LIMIT 1")
        agent = cursor.fetchone()[0]
        orders.append((
            fake.uuid4(),
            customer,
            fake.date_time_between_dates(datetime_start='-1y'),
            item,
            random.randint(1, 10),
            agent,
            fake.word(ext_word_list=['Picking', 'Awaiting Pickup', 'Quality Check', 'Packing', 'Shipped', 'Delivered', 'Returned']),
            fake.word(ext_word_list=['Small', 'Medium', 'Large', 'X-Large', 'Kids'])
        ))
    cursor.executemany(
        f"INSERT INTO {dbname}.orders (orderid, customerid, orderdate, orderitem, qty, agent, orderstage, size) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        orders
    )
    conn.close()

    conn = get_connection(database=dbname, host=host, port=port, user=user, password=password)
    conn.autocommit = True
    cursor = conn.cursor()
    calls = []
    for _ in range(records):
        cursor.execute(f"SELECT agent_id FROM {dbname}.agents ORDER BY random() LIMIT 1")
        agent = cursor.fetchone()[0]
        cursor.execute(f"SELECT customerid FROM {dbname}.customers ORDER BY random() LIMIT 1")
        customer = cursor.fetchone()[0]
        calls.append((
            fake.uuid4(),
            agent,
            customer,
            fake.date_time_between_dates(datetime_start='-1y'),
            fake.word(ext_word_list=['Billing Enquiry', 'Complaint', 'Bill Payment', 'Technical Support', 'Other']),
            fake.random_int(min=50, max=500),
            random.choice([True, False]),
            fake.random_int(min=10, max=20),
            fake.word(ext_word_list=['Research', 'Documentation', 'Manager Escalation', 'Break', 'Transfer']),
            "Inbound" if random.random() < 0.8 else "Outbound",
            fake.word(ext_word_list=['Happy', 'Frustrated', 'Angry', 'Distressed', 'Satisfied', 'Disappointed', 'Relieved', 'Betrayed', 'Joyful'])
        ))
    cursor.executemany(
        f"INSERT INTO {dbname}.calls (callref, agent, customer, date, reason, duration, resolved, answertime, reasoncode, calldirection, sentiment) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        calls
    )
    conn.close()
