import pandas as pd
from sqlalchemy import create_engine, text
import cred as c

engine = create_engine(
    f'postgresql://{c.pg_userid}:{c.pg_password}@{c.pg_host}/{c.pg_db}', 
    connect_args = {'options': '-c search_path=usozmed,public', 'keepalives_idle': 120},
    pool_size=1, 
    max_overflow=0,
    execution_options={ 'isolation_level': 'AUTOCOMMIT' }
)

sql = """
-- create indexes on foreign keys
CREATE INDEX i01 ON Forum (ModeratorPersonId);
CREATE INDEX i02 ON Forum_hasMember_Person (ForumId);
CREATE INDEX i03 ON Forum_hasMember_Person (PersonId);
CREATE INDEX i04 ON Forum_hasTag_Tag (ForumId);
CREATE INDEX i05 ON Forum_hasTag_Tag (TagId);
CREATE INDEX i06 ON Person_knows_Person (Person1Id);
CREATE INDEX i07 ON Person_knows_Person (Person2Id);
CREATE INDEX i08 ON Person_likes_Message (PersonId);
CREATE INDEX i09 ON Person_likes_Message (MessageId);
CREATE INDEX i10 ON University (LocationPlaceId);
CREATE INDEX i11 ON Company (LocationPlaceId);
CREATE INDEX i12 ON person (LocationCityId);
CREATE INDEX i13 ON Person_workAt_Company (PersonId);
CREATE INDEX i14 ON Person_workAt_Company (CompanyId);
CREATE INDEX i15 ON Person_hasInterest_Tag (PersonId);
CREATE INDEX i16 ON Person_hasInterest_Tag (TagId);
CREATE INDEX i17 ON Person_studyAt_University (PersonId);
CREATE INDEX i18 ON Person_studyAt_University (UniversityId);
CREATE INDEX i19 ON Message (CreatorPersonId);
CREATE INDEX i20 ON Message (LocationCountryId);
CREATE INDEX i21 ON Message (ContainerForumId);
CREATE INDEX i22 ON Message (ParentMessageId);
CREATE INDEX i23 ON Message_hasTag_Tag (MessageId);
CREATE INDEX i24 ON Message_hasTag_Tag (TagId);
CREATE INDEX i25 ON Tag (TypeTagClassId);
CREATE INDEX i26 ON TagClass (SubclassOfTagClassId);
"""
with engine.connect() as con:
    con.execute(text(sql))

print("ok")
