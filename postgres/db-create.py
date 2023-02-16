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
# 01 TagClass
# -------------------------------------------------
sql1 = f"""
drop table if exists TagClass;
"""

sql2 = f"""
CREATE TABLE TagClass (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    url text NOT NULL,
    SubclassOfTagClassId bigint -- null for the root TagClass (Thing)
);
"""

sql3 = f"""
copy TagClass from '{csvdir}/TagClass.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))


# -------------------------------------------------
# 02 Tag
# -------------------------------------------------
sql1 = f"""
drop table if exists Tag;
"""

sql2 = f"""
CREATE TABLE Tag (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    url text NOT NULL,
    TypeTagClassId bigint NOT NULL
);
"""

sql3 = f"""
copy Tag from '{csvdir}/Tag.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))

# -------------------------------------------------
# 03 Country
# -------------------------------------------------
sql1 = f"""
drop table if exists Country;
"""

sql2 = f"""
CREATE TABLE Country (
    id bigint PRIMARY KEY,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    PartOfContinentId bigint
);
"""

sql3 = f"""
copy Country from '{csvdir}/Country.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))

# -------------------------------------------------
# 04 City
# -------------------------------------------------
sql1 = f"""
drop table if exists City;
"""

sql2 = f"""
CREATE TABLE City (
    id bigint PRIMARY KEY,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    PartOfCountryId bigint
);
"""

sql3 = f"""
copy City from '{csvdir}/City.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))

# -------------------------------------------------
# 05 Company
# -------------------------------------------------
sql1 = f"""
drop table if exists Company;
"""

sql2 = f"""
CREATE TABLE Company (
    id bigint PRIMARY KEY,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    LocationPlaceId bigint NOT NULL
);
"""

sql3 = f"""
copy Company from '{csvdir}/Company.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))

# -------------------------------------------------
# 06 University
# -------------------------------------------------
sql1 = f"""
drop table if exists University;
"""

sql2 = f"""
CREATE TABLE University (
    id bigint PRIMARY KEY,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    LocationPlaceId bigint NOT NULL
);
"""

sql3 = f"""
copy University from '{csvdir}/University.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))


# -------------------------------------------------
# 07 Person
# -------------------------------------------------
sql1 = f"""
drop table if exists Person;
"""

sql2 = f"""
CREATE TABLE Person (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    firstName text NOT NULL,
    lastName text NOT NULL,
    gender text NOT NULL,
    birthday date NOT NULL,
    locationIP text NOT NULL,
    browserUsed text NOT NULL,
    LocationCityId bigint NOT NULL,
    speaks text NOT NULL,
    email text NOT NULL
)
"""

sql3 = f"""
copy Person from '{csvdir}/Person.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))


# -------------------------------------------------
# 08 Forum
# -------------------------------------------------
sql1 = f"""
drop table if exists Forum;
"""

sql2 = f"""
CREATE TABLE Forum (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    title text NOT NULL,
    ModeratorPersonId bigint -- can be null as its cardinality is 0..1
);
"""

sql3 = f"""
copy Forum from '{csvdir}/Forum.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))


# -------------------------------------------------
# 09 Forum_hasMember_Person
# -------------------------------------------------
sql1 = f"""
drop table if exists Forum_hasMember_Person;
"""

sql2 = f"""
CREATE TABLE Forum_hasMember_Person (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
);
"""

sql3 = f"""
copy Forum_hasMember_Person from '{csvdir}/Forum_hasMember_Person.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))


# -------------------------------------------------
# 10 Forum_hasTag_Tag
# -------------------------------------------------
sql1 = f"""
drop table if exists Forum_hasTag_Tag;
"""

sql2 = f"""
CREATE TABLE Forum_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    TagId bigint NOT NULL
);
"""

sql3 = f"""
copy Forum_hasTag_Tag from '{csvdir}/Forum_hasTag_Tag.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))



# -------------------------------------------------
# 11 Person_hasInterest_Tag
# -------------------------------------------------
sql1 = f"""
drop table if exists Person_hasInterest_Tag;
"""

sql2 = f"""
CREATE TABLE Person_hasInterest_Tag (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    TagId bigint NOT NULL
);
"""

sql3 = f"""
copy Person_hasInterest_Tag from '{csvdir}/Person_hasInterest_Tag.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))


# -------------------------------------------------
# 12 Person_studyAt_University
# -------------------------------------------------
sql1 = f"""
drop table if exists Person_studyAt_University;
"""

sql2 = f"""
CREATE TABLE Person_studyAt_University (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    UniversityId bigint NOT NULL,
    classYear int NOT NULL
);
"""

sql3 = f"""
copy Person_studyAt_University from '{csvdir}/Person_studyAt_University.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))


# -------------------------------------------------
# 13 Person_workAt_Company
# -------------------------------------------------
sql1 = f"""
drop table if exists Person_workAt_Company;
"""

sql2 = f"""
CREATE TABLE Person_workAt_Company (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CompanyId bigint NOT NULL,
    workFrom int NOT NULL
);
"""

sql3 = f"""
copy Person_workAt_Company from '{csvdir}/Person_workAt_Company.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))


# -------------------------------------------------
# 14 Person_knows_Person
# -------------------------------------------------
sql1 = f"""
drop table if exists Person_knows_Person;
"""

sql2 = f"""
CREATE TABLE Person_knows_Person (
    creationDate timestamp with time zone NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL,
    PRIMARY KEY (Person1id, Person2id)
); 
"""

sql3 = f"""
copy Person_knows_Person from '{csvdir}/Person_knows_Person.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))



# -------------------------------------------------
# 15 Message
# -------------------------------------------------
sql1 = f"""
drop table if exists Message;
"""

sql2 = f"""
CREATE TABLE Message (
    creationDate timestamp with time zone NOT NULL,
    id bigint,
    language varchar(80),
    content varchar(2000),
    imageFile varchar(80),
    locationIP varchar(80) NOT NULL,
    browserUsed varchar(80) NOT NULL,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    ContainerForumId bigint,
    LocationCountryId bigint NOT NULL,
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
# 16 Person_likes_Message
# -------------------------------------------------
sql1 = f"""
drop table if exists Person_likes_Message;
"""

sql2 = f"""
CREATE TABLE Person_likes_Message (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    MessageId bigint NOT NULL
);
"""

sql3 = f"""
copy Person_likes_Message from '{csvdir}/Person_likes_Message.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))

# -------------------------------------------------
# 17 Message_hasTag_Tag
# -------------------------------------------------
sql1 = f"""
drop table if exists Message_hasTag_Tag;
"""

sql2 = f"""
CREATE TABLE Message_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    MessageId bigint NOT NULL,
    TagId bigint NOT NULL
);
"""

sql3 = f"""
copy Message_hasTag_Tag from '{csvdir}/Message_hasTag_Tag.csv' delimiter '|' csv header;
"""

with engine.connect() as con:
    con.execute(text(sql1))
    con.execute(text(sql2))
    con.execute(text(sql3))


# -------------------------------------------------
print("ok")
# -------------------------------------------------


