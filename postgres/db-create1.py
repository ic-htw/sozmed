
import pandas as pd
from sqlalchemy import create_engine, text
import cred as c
csvdir = '/var/lib/postgresql/csv/sozmed'

engine = create_engine(
    f'postgresql://{c.pg_userid}:{c.pg_password}@{c.pg_host}/{c.pg_db}', 
    connect_args = {'options': '-c search_path=usozmed,public', 'keepalives_idle': 120},
    pool_size=1, 
    max_overflow=0,
    execution_options={ 'isolation_level': 'AUTOCOMMIT' }
)

# -------------------------------------------------
# 15 Message
# -------------------------------------------------
sql1 = f"""
drop table if exists Message;
"""

sql2 = f"""
CREATE TABLE Message (
    creationDate timestamp with time zone,
    id bigint,
    language varchar(80),
    content varchar(2000),
    imageFile varchar(80),
    locationIP varchar(80),
    browserUsed varchar(80),
    length int NOT NULL,
    CreatorPersonId bigint,
    ContainerForumId bigint,
    LocationCountryId bigint,
    ParentMessageId bigint
);
"""

sql3 = f"""
copy Message from '{csvdir}/Message.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))

    

# -------------------------------------------------
print("ok")
# -------------------------------------------------


