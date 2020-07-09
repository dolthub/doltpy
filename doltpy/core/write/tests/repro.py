from sqlalchemy import Table, MetaData, Column, String, Integer
import sqlalchemy
from retry import retry
from doltpy.etl.sql_sync.tests.fixtures.mysql import MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD, MYSQL_PORT
from doltpy.etl.sql_sync.tests.fixtures.db_fixtures_helper import engine_helper
# Just needs to be an empty repo


def test_repro(init_empty_test_repo):
    repo = init_empty_test_repo
    repo.sql_server()

    engine = repo.get_engine()

    if 'test' in [table.name for table in repo.ls()]:
        with engine.connect() as conn:
            conn.execute('drop table test')

    @retry(exceptions=(sqlalchemy.exc.OperationalError,  sqlalchemy.exc.DatabaseError), delay=2, tries=10)
    def verify_connection():
        conn = engine.connect()
        conn.close()
        return engine

    verify_connection()
    metadata = MetaData(bind=engine)
    table = Table('test', metadata, Column('id', Integer, primary_key=True), Column('name', String(16)))
    table.create()

    print('Tables are:\n{}'.format(repo.ls()))

    new_engine = repo.get_engine()
    new_metadata = MetaData(bind=new_engine, reflect=True)
    print('Tables are:\n{}'.format(table.name for table in new_metadata.tables))


def test_repro_mysql(mysql_with_table, docker_ip, docker_services):
    engine, _ = mysql_with_table

    insert = '''
    CREATE TABLE `test` (
      `id` int NOT NULL COMMENT 'tag:4490',
      `name` VARCHAR(16) COMMENT 'tag:4490',
      PRIMARY KEY (`id`)
    )
    '''

    with engine.connect() as conn:
        conn.execute(insert)

    new_engine = engine_helper('mysql+mysqlconnector',
                               MYSQL_USER,
                               MYSQL_PASSWORD,
                               docker_ip,
                               docker_services.port_for('mysql', MYSQL_PORT),
                               MYSQL_DATABASE)
    new_metadata = MetaData(bind=new_engine, reflect=True)
    print('Tables are:\n{}'.format([table for table in new_metadata.tables]))
