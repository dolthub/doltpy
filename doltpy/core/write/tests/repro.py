from sqlalchemy import Table, MetaData, Column, String, Integer
from doltpy.core import Dolt
import sqlalchemy
from retry import retry
# Just needs to be an empty repo
TEST_REPO_PATH = '/Users/oscarbatori/Documents/dolt_sqlalchemy_repro'


def test_repro():
    repo = Dolt(TEST_REPO_PATH)
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