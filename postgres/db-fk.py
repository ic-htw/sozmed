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
ALTER TABLE City ADD FOREIGN KEY (PartOfCountryId) REFERENCES Country(Id);
ALTER TABLE Company ADD FOREIGN KEY (LocationPlaceId) REFERENCES Country(id);
ALTER TABLE University ADD FOREIGN KEY (LocationPlaceId) REFERENCES City(id);

ALTER TABLE Tag ADD FOREIGN KEY (TypeTagClassId) REFERENCES TagClass(id);
ALTER TABLE TagClass ADD FOREIGN KEY (SubclassOfTagClassId) REFERENCES TagClass(id);

ALTER TABLE Person ADD FOREIGN KEY (LocationCityId) REFERENCES City(id);

ALTER TABLE Person_hasInterest_Tag ADD FOREIGN KEY (TagId) REFERENCES Tag(id);
ALTER TABLE Person_hasInterest_Tag ADD FOREIGN KEY (PersonId) REFERENCES Person(id);
ALTER TABLE Person_studyAt_University ADD FOREIGN KEY (UniversityId) REFERENCES University(Id);
ALTER TABLE Person_workAt_Company ADD FOREIGN KEY (CompanyId) REFERENCES Company(id);

ALTER TABLE Person_knows_Person ADD FOREIGN KEY (Person1Id) REFERENCES Person(id) ON DELETE CASCADE;
ALTER TABLE Person_knows_Person ADD FOREIGN KEY (Person2Id) REFERENCES Person(id) ON DELETE CASCADE;
ALTER TABLE Person_likes_Message ADD FOREIGN KEY (PersonId) REFERENCES Person(id) ON DELETE CASCADE;
ALTER TABLE Person_likes_Message ADD FOREIGN KEY (id) REFERENCES Message(id) ON DELETE CASCADE;

ALTER TABLE Person_studyAt_University ADD FOREIGN KEY (PersonId) REFERENCES Person(id) ON DELETE CASCADE;
ALTER TABLE Person_workAt_Company ADD FOREIGN KEY (PersonId) REFERENCES Person(id) ON DELETE CASCADE;


ALTER TABLE Forum_hasTag_Tag ADD FOREIGN KEY (TagId) REFERENCES Tag(id);
ALTER TABLE Message ADD FOREIGN KEY (LocationCountryId) REFERENCES place;
ALTER TABLE Message_hasTag_Tag ADD FOREIGN KEY (TagId) REFERENCES Tag(id);

-- node should not be deleted upon deletion of the endpoint
-- can be null, so not a valid FK!
-- ALTER TABLE Forum ADD FOREIGN KEY (ModeratorPersonId) REFERENCES Person(id);

-- dynamic endpoints (edge should be deleted when the endpoint is deleted)
ALTER TABLE Forum_hasMember_Person ADD FOREIGN KEY (ForumId) REFERENCES Forum(id) ON DELETE CASCADE;
ALTER TABLE Forum_hasMember_Person ADD FOREIGN KEY (PersonId) REFERENCES Person(id) ON DELETE CASCADE;
ALTER TABLE Forum_hasTag_Tag ADD FOREIGN KEY (ForumId) REFERENCES Forum(id) ON DELETE CASCADE;

ALTER TABLE Message ADD FOREIGN KEY (CreatorPersonId) REFERENCES Person(id) ON DELETE CASCADE;
ALTER TABLE Message ADD FOREIGN KEY (ContainerForumId) REFERENCES Forum(id) ON DELETE CASCADE;
ALTER TABLE Message ADD FOREIGN KEY (ParentMessageId) REFERENCES Message(id) ON DELETE CASCADE;
ALTER TABLE Message_hasTag_Tag ADD FOREIGN KEY (id) REFERENCES Message(id) ON DELETE CASCADE;

"""
with engine.connect() as con:
    con.execute(sql)

print("ok")