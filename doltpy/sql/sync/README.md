## Background and Motivation

Dolt is a relational database that uses a commit graph as its underlying data store. It emulates Git's model of versioning by creating a commit in that commit graph when prompted via either command line interface, SQL, or some other API.

This is distinct from existing relational database solutions, such as MySQL, Postgresql, Oracle, that have a "last write wins" model of versioning. A cell takes on the value set by the last query that modified it. Some of these databases, such as Microsoft SQL Sever and Maria DB, also support `AS OF` functionality that stores the history of values and exposes a syntax for querying them. This requires explicit configuration. Additionally, these relational database implementations offer various sorts of backup and recovery mechanisms that, at a very high level, take periodic snapshots which can be restored.

This document shows how to use Dolt to create version history of an existing relational database without having to actually modify that database. We use our Python API, [Doltpy](https://github.com/dolthub/docs/tree/bfdf7d8c4c511940b3281abe0290c8eb4097e6c0/reference/python/README.md), to achieve this. The setup can visualized as follows:

![Sync to Dolt](./sql_sync_diagram.png)

The left hand side shows a schematic for the how each sync corresponds to a commit in Dolt, and the right hand side the query interface presented to the user via Dolt SQL. We can now "time travel" through our database history.

## Scope and Prerequisites

This guide will explain how to sync data to and from Dolt, and either Postgres or MySQL. Currently automated schema sync is supported only when syncing from another database to Dolt.

This guide assumes that you have both Dolt and Doltpy installed, and that you are somewhat familiar with both Dolt and Python. Checkout the [installation guide](https://github.com/dolthub/docs/tree/bfdf7d8c4c511940b3281abe0290c8eb4097e6c0/tutorials/installation/README.md) if you haven't install Dolt or Doltpy.

## MySQL

Our current support for syncing MySQL and Dolt allows users to sync data from MySQL to Dolt, or from Dolt to MySQL. We also support syncing a schema from MySQL into Dolt.

### MySQL to Dolt

#### Setup

In order to perform the sync we need some objects that provide connections to the relevant databases, and we need to start any database servers needed. Let's assume we are running MySQL on `mysql_host` and `mysql_port`. We can crate the required database engine object, and an object to represent our Dolt database as follows:

```python
from doltpy.core import Dolt
import sqlalchemy as sa

# Setup objects to represents source and target databases, start Dolt SQL Server
dolt = Dolt.clone('my-org/myback')
dolt.sql_server()
mysql_engine = sa.create_engine(
    '{dialect}://{user}:{password}@{host}:{port}/{database}'.format(
        dialect='mysql+mysqlconnector',
        user=mysql_user,
        password=mysql_password,
        host=mysql_host,
        port=mysql_port,
        database=mysql_database
    )
)
```

#### Schema

The code for syncing the schema to Dolt once the database engine object and Dolt object are created is straight forward. We simply execute a generic function with engine appropriate parameters that take care of mapping types that Dolt does not support, which for MySQL is currently only JSON:

```python
from doltpy.etl.sql_sync import sync_schema_to_dolt, MYSQL_TO_DOLT_TYPE_MAPPING

sync_schema_to_dolt(mysql_engine,
                    dolt,
                    {'revenue_estimates': 'revenue_estimates'},
                    MYSQL_TO_DOLT_TYPE_MAPPINGS)
```

#### Data

Syncing the data to Dolt is similarly straight forward, and we use the same design pattern: pass implementation specific parameters, in this case reader and writer functions, to a generic procedure for syncing to Dolt:

```python
from doltpy.etl.sql_sync import sync_to_dolt, get_dolt_target_writer, get_mysql_source_reader

# Execute the sync
sync_to_dolt(get_mysql_source_reader(mysql_engine),
             get_dolt_target_writer(dolt),
             {'revenue_estimates': 'revenue_estimates'})
```

Finally, we might want to stop the Dolt SQL server running:

```python
dolt.sql_server_stop()
```

Relational databases have a straightforward concept of state, that is they simply capture the last value written to a given cell. We provide a simple reader that just captures the state of the table, and passes it to a simple writer for creating a commit. Note, Dolt will handle discerning what has changed, and commit _only the changes_ which will allow users to see diffs across syncs.

### Dolt to MySQL

We now provide an example of going from Dolt to a MySQL instance. We again assume that the MySQL instance is running at `mysql_host` on port `mysql_port`, and we assume the existence of `mysql_engine` and `dolt` objects that we created in the Setup section above to keep the example succinct.

#### Data

Since we do not currently support copying a Dolt schema to MySQL, we jump straight to syncing data. We use the same design pattern, passing implementation specific functions to a generic procedure for syncing from Dolt:

```python
from doltpy.etl.sql_sync import sync_from_dolt, get_mysql_target_writer, get_dolt_source_reader

sync_from_dolt(get_dolt_source_reader(dolt_repo, get_dolt_table_reader()),
               get_mysql_target_writer(mysql_engine),
               {'revenue_estimates': 'revenue_estimates'})
```

In order to facilitate user defined behavior of the actual database interactions, the sync function takes functions for reading and writing. Here we use the library's default readers and writers for Dolt and MySQL respectively. The Dolt source reader just reads the database at the latest commit, defaulting to the tip of `master`, and the target writer just updates that state in the target MySQL instance. Below we dive deeper into how we might go about implementing custom behavior.

Recall that Dolt databases are a commit graph, and so each commit is essentially a database state, thus we replicate precisely that state, including dropping primary keys that might have been deleted in that commit.

## Postgres

Our current support for syncing Postgresql and Dolt allows users to sync data from Postgresql to Dolt, or from Dolt to Postgresql. We also support syncing a schema from Postgresql into Dolt.

### Postgres to Dolt

Syncing to Postgres is similar. In the previous section we showed code snippets that use function parameters to specify implementation specific behavior:

```python
from doltpy.etl.sql_sync import sync_to_dolt, get_DB_target_writer, get_dolt_source_reader

sync_from_dolt(get_dolt_source_reader(dolt, get_dolt_table_reader()),
               get_DB_target_writer(mysql_engine),
               {'revenue_estimates': 'revenue_estimates'})
```

We simply replace use `postgres` instead of the placeholder `DB` in order to sync to Postgres.

#### Setup

As in the MySQL section, we need some objects to represent the Postgres connection and the Dolt database. We assume Postgres is running `postgres_host` on port `postgres_port`:

```python
from doltpy.core import Dolt
import sqlalchemy as sa

dolt_repo = Dolt.clone('my-org/analyst-estimates')
dolt_repo.sql_server()
postgres_engine = sa.create_engine(
    '{dialect}://{user}:{password}@{host}:{port}/{database}'.format(
        dialect='postgresql',
        user=postgres_user,
        password=postgres_password,
        host=postgres_host,
        port=postgres_port,
        database=postgres_database
    )
)
```

#### Schema

We can now use the same generic `sync_schema_to_dolt` function with implementation specific parameters to sync the Postgres schema to Dolt:

```python
from doltpy.etl.sql_sync import sync_schema_to_dolt, POSTGRES_TO_DOLT_TYPE_MAPPINGS

sync_schema_to_dolt(postgres_engine,
                    dolt,
                    {'revenue_estimates': 'revenue_estimates'},
                    POSTGRES_TO_DOLT_TYPE_MAPPINGS)
```

#### Data

To sync data we again use `sync_to_dolt`, this time with Postgres specific function parameters to get the correct behavior:

```python
from doltpy.etl.sql_sync import sync_to_dolt, get_postgres_target_writer, get_dolt_source_reader

sync_to_dolt(get_postgres_source_reader(postgres_engine),
             get_dolt_target_writer(dolt),
             {'revenue_estimates': 'revenue_estimates'})
```

### Dolt to Postgres

We again assume that Postgres is running on `postgres_host` at port `postgres_port`, and reuse the objects we defined in the setup section. Because we don't yet support syncing a Postgres schema to Dolt, we jump straight into getting data from Dolt into Postgres.

#### Data

As with syncing to Dolt from Postgres, we employ a generic method parameterized with database implementation specific parameters:

```python
from doltpy.etl.sql_sync import sync_from_dolt, get_postgres_target_writer, get_dolt_source_reader

sync_from_dolt(get_dolt_source_reader(dolt, get_dolt_table_reader()),
               get_postgres_target_writer(postgres_engine),
               {'revenue_estimates': 'revenue_estimates'})
```

## Oracle

Our current support for Oracle is data-only, which is to say we support syncing only data, and any schema must be manually created before schema sync can occur.

### Oracle to Dolt

We again employ the same pattern for syncing form Oracle to Dolt that we used in the MySQL and Postgres sections.

#### Setup

As in the Postgres and MySQL sections, we need some objects to represent the Oracle connection and the Dolt database. We assume Oracle is running `oracle_host` on port `oracle_port`:

```python
from doltpy.core import Dolt
import sqlalchemy as sa
import cx_Oracle

dolt = Dolt.clone('my-org/analyst-estimates')
dolt.sql_server()

def _oracle_connection_helper:
    return cx_Oracle.connect('oracle_user', 'oracle_pwd', '{}:{}/{}'.format('oracle_host', 1521, 'oracle_db'))

engine = create_engine('oracle+cx_oracle://', creator=_oracle_connection_helper)
```

#### Data

To sync data we again use `sync_to_dolt`, this time with Oracle specific function parameters to get the correct behavior:

```python
from doltpy.etl.sql_sync import sync_to_dolt, get_oracle_target_writer, get_dolt_source_reader

sync_to_dolt(get_postgres_source_reader(oracle_engine),
             get_dolt_target_writer(dolt),
             {'revenue_estimates': 'revenue_estimates'})
```

### Dolt to Oracle

We again assume that Oracle is running on `oracle_host` at port `oracle_port`, and reuse the objects we defined in the setup section. Because we don't yet support syncing an Oracle schema to Dolt, we jump straight into getting data from Dolt into Oracle.

#### Data

As with syncing to Dolt from Postgres, we employ a generic method parameterized with database implementation specific parameters:

```python
from doltpy.etl.sql_sync import sync_from_dolt, get_oracle_target_writer, get_dolt_source_reader

sync_from_dolt(get_dolt_source_reader(dolt, get_dolt_table_reader()),
               get_oracle_target_writer(oracle_engine),
               {'revenue_estimates': 'revenue_estimates'})
```

## Customizing Behavior

Diving into the code that does the actual sync, one thing that jumps out is how simple it is \(doc strings omitted for clarity\):

```python
# Types that reflect the different nature of the syncs
DoltTableUpdate = Tuple[Iterable[dict], Iterable[dict]]
TableUpdate = Iterable[dict]

# For using Dolt as the target
DoltAsTargetUpdate = Mapping[str, TableUpdate]
DoltAsTargetReader = Callable[[List[str]], DoltAsTargetUpdate]
DoltAsTargetWriter = Callable[[DoltAsTargetUpdate], None]

# For using Dolt as the source
DoltAsSourceUpdate = Mapping[str, DoltTableUpdate]
DoltAsSourceReader = Callable[[List[str]], DoltAsSourceUpdate]
DoltAsSourceWriter = Callable[[DoltAsSourceUpdate], None]


def sync_to_dolt(source_reader: DoltAsTargetReader, target_writer: DoltAsTargetWriter, table_map: Mapping[str, str]):
    _sync_helper(source_reader, target_writer, table_map)


def sync_from_dolt(source_reader: DoltAsSourceReader, target_writer: DoltAsSourceWriter, table_map: Mapping[str, str]):
    _sync_helper(source_reader, target_writer, table_map)


def _sync_helper(source_reader, target_writer, table_map: Mapping[str, str]):
    to_sync = source_reader(list(table_map.keys()))
    remapped = {table_map[source_table]: source_data for source_table, source_data in to_sync.items()}
    target_writer(remapped)
```

The "interface" that the two databases communicate over is strikingly simple. `Mapping[str, TableUpdate]` is a mapping from table name to a list of `dict` instances, each one representing a row. Users are free to customize behavior by providing a function that reads from their database \(Dolt, MySQL, Postgres\), and produces a list of `dict` values that match the schema of the target database.

## Future Work

We are excited by the possibilities a database with Dolt's unique features creates for data engineering workflows. Next up the sync is expanding to more relational database implementations \(MS SQL Server and Oracle\), and supporting syncing a schema from Dolt to any of the supported database implementations.
