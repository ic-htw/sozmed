drop table Message_hasTag_Tag;
drop table Person_likes_Message;
drop table Person_knows_Person;
drop table Person_workAt_Company;
drop table Person_studyAt_University;
drop table Person_hasInterest_Tag;
drop table Forum_hasTag_Tag;
drop table Forum_hasMember_Person;
drop table Message;
drop table Forum;
drop table Person;
drop table University;
drop table Company;
drop table City;
drop table Country;
drop table Tag;
drop table TagClass;


---------------------------------------------------
--01 TagClass
---------------------------------------------------
CREATE TABLE TagClass (
    id number PRIMARY KEY,
    name varchar2(500) NOT NULL,
    url varchar2(500) NOT NULL,
    SubclassOfTagClassId number -- null for the root TagClass (Thing)
);

---------------------------------------------------
--02 Tag
---------------------------------------------------
CREATE TABLE Tag (
    id number PRIMARY KEY,
    name varchar2(500) NOT NULL,
    url varchar2(500) NOT NULL,
    TypeTagClassId number NOT NULL
);

---------------------------------------------------
--03 Country
---------------------------------------------------
CREATE TABLE Country (
    id number PRIMARY KEY,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    PartOfContinentId number
);

---------------------------------------------------
--04 City
---------------------------------------------------
CREATE TABLE City (
    id number PRIMARY KEY,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    PartOfCountryId number
);

---------------------------------------------------
--05 Company
---------------------------------------------------
CREATE TABLE Company (
    id number PRIMARY KEY,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    LocationPlaceId number NOT NULL
);

---------------------------------------------------
--06 University
---------------------------------------------------
CREATE TABLE University (
    id number PRIMARY KEY,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    LocationPlaceId number NOT NULL
);


---------------------------------------------------
--07 Person
---------------------------------------------------
CREATE TABLE Person (
    creationDate timestamp NOT NULL,
    id number PRIMARY KEY,
    firstName varchar2(500) NOT NULL,
    lastName varchar2(500) NOT NULL,
    gender varchar2(500) NOT NULL,
    birthday date NOT NULL,
    locationIP varchar2(500) NOT NULL,
    browserUsed varchar2(500) NOT NULL,
    LocationCityId number NOT NULL,
    speaks varchar2(500) NOT NULL,
    email varchar2(500) NOT NULL
);

---------------------------------------------------
--08 Forum
---------------------------------------------------
CREATE TABLE Forum (
    creationDate timestamp NOT NULL,
    id number PRIMARY KEY,
    title varchar2(500) NOT NULL,
    ModeratorPersonId number -- can be null as its cardinality is 0..1
);


---------------------------------------------------
--09 Forum_hasMember_Person
---------------------------------------------------
CREATE TABLE Forum_hasMember_Person (
    creationDate timestamp NOT NULL,
    ForumId number NOT NULL,
    PersonId number NOT NULL
);


---------------------------------------------------
--10 Forum_hasTag_Tag
---------------------------------------------------
CREATE TABLE Forum_hasTag_Tag (
    creationDate timestamp NOT NULL,
    ForumId number NOT NULL,
    TagId number NOT NULL
);


---------------------------------------------------
--11 Person_hasInterest_Tag
---------------------------------------------------
CREATE TABLE Person_hasInterest_Tag (
    creationDate timestamp NOT NULL,
    PersonId number NOT NULL,
    TagId number NOT NULL
);


---------------------------------------------------
--12 Person_studyAt_University
---------------------------------------------------
CREATE TABLE Person_studyAt_University (
    creationDate timestamp NOT NULL,
    PersonId number NOT NULL,
    UniversityId number NOT NULL,
    classYear int NOT NULL
);


---------------------------------------------------
--13 Person_workAt_Company
---------------------------------------------------
CREATE TABLE Person_workAt_Company (
    creationDate timestamp NOT NULL,
    PersonId number NOT NULL,
    CompanyId number NOT NULL,
    workFrom int NOT NULL
);


---------------------------------------------------
--14 Person_knows_Person
---------------------------------------------------
CREATE TABLE Person_knows_Person (
    creationDate timestamp NOT NULL,
    Person1id number NOT NULL,
    Person2id number NOT NULL,
    PRIMARY KEY (Person1id, Person2id)
); 


---------------------------------------------------
--15 Message
---------------------------------------------------
CREATE TABLE Message (
    creationDate timestamp NOT NULL,
    id number PRIMARY KEY,
    language varchar(80),
    content varchar(2000),
    imageFile varchar(80),
    locationIP varchar(80) NOT NULL,
    browserUsed varchar(80) NOT NULL,
    length int NOT NULL,
    CreatorPersonId number NOT NULL,
    ContainerForumId number,
    LocationCountryId number NOT NULL,
    ParentMessageId number
);
   
---------------------------------------------------
--16 Person_likes_Message
---------------------------------------------------
CREATE TABLE Person_likes_Message (
    creationDate timestamp NOT NULL,
    PersonId number NOT NULL,
    MessageId number NOT NULL
);

---------------------------------------------------
--17 Message_hasTag_Tag
---------------------------------------------------
CREATE TABLE Message_hasTag_Tag (
    creationDate timestamp NOT NULL,
    MessageId number NOT NULL,
    TagId number NOT NULL
);
