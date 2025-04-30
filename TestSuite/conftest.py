import pytest
import mysql.connector
import cx_Oracle

#Source db_conn
@pytest.fixture(scope='session')
def MySQL_Conn():
    source_db_conn=mysql.connector.connect(user='root',password='admin',database='source',host='localhost')

    yield source_db_conn

    source_db_conn.close()

#Target db_conn
@pytest.fixture(scope='session')
def Oracle_Conn():
    target_db_conn=cx_Oracle.connect("c##target/target@suri:1521/xe")

    yield target_db_conn

    target_db_conn.close()