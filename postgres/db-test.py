import pandas as pd
from sqlalchemy import create_engine, text
import cred as c

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

engine = create_engine(
    f'postgresql://{c.pg_userid}:{c.pg_password}@{c.pg_host}/{c.pg_db}', 
    connect_args = {'options': '-c search_path=usozmed,public', 'keepalives_idle': 120},
    pool_size=1, 
    max_overflow=0,
    execution_options={ 'isolation_level': 'AUTOCOMMIT' }
)

q = """
select
  relname as table_name,
  n_live_tup as row_count
from pg_stat_user_tables
where schemaname = 'usozmed'
order by schemaname, relname;
"""

# q = "select * from message limit 20;"
# q = "select count(*) from message where not parentmessageid is null;"
# q = "select count(*) from message;"

with engine.connect() as con:
    df = pd.read_sql_query(text(q), con)
print(df)