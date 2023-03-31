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
    id bigint PRIMARY KEY,
    name nvarchar(5000) NOT NULL,
    url nvarchar(5000) NOT NULL,
    SubclassOfTagClassId bigint -- null for the root TagClass (Thing)
);

---------------------------------------------------
--02 Tag
---------------------------------------------------
CREATE TABLE Tag (
    id bigint PRIMARY KEY,
    name nvarchar(5000) NOT NULL,
    url nvarchar(5000) NOT NULL,
    TypeTagClassId bigint NOT NULL
);

---------------------------------------------------
--03 Country
---------------------------------------------------
CREATE TABLE Country (
    id bigint PRIMARY KEY,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    PartOfContinentId bigint
);

---------------------------------------------------
--04 City
---------------------------------------------------
CREATE TABLE City (
    id bigint PRIMARY KEY,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    PartOfCountryId bigint
);

---------------------------------------------------
--05 Company
---------------------------------------------------
CREATE TABLE Company (
    id bigint PRIMARY KEY,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    LocationPlaceId bigint NOT NULL
);

---------------------------------------------------
--06 University
---------------------------------------------------
CREATE TABLE University (
    id bigint PRIMARY KEY,
    name varchar(256) NOT NULL,
    url varchar(256) NOT NULL,
    LocationPlaceId bigint NOT NULL
);


---------------------------------------------------
--07 Person
---------------------------------------------------
CREATE TABLE Person (
    creationDate timestamp NOT NULL,
    id bigint PRIMARY KEY,
    firstName nvarchar(5000) NOT NULL,
    lastName nvarchar(5000) NOT NULL,
    gender nvarchar(5000) NOT NULL,
    birthday date NOT NULL,
    locationIP nvarchar(5000) NOT NULL,
    browserUsed nvarchar(5000) NOT NULL,
    LocationCityId bigint NOT NULL,
    speaks nvarchar(5000) NOT NULL,
    email nvarchar(5000) NOT NULL
);

---------------------------------------------------
--08 Forum
---------------------------------------------------
CREATE TABLE Forum (
    creationDate timestamp NOT NULL,
    id bigint PRIMARY KEY,
    title nvarchar(5000) NOT NULL,
    ModeratorPersonId bigint -- can be null as its cardinality is 0..1
);


---------------------------------------------------
--09 Forum_hasMember_Person
---------------------------------------------------
CREATE TABLE Forum_hasMember_Person (
    creationDate timestamp NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
);


---------------------------------------------------
--10 Forum_hasTag_Tag
---------------------------------------------------
CREATE TABLE Forum_hasTag_Tag (
    creationDate timestamp NOT NULL,
    ForumId bigint NOT NULL,
    TagId bigint NOT NULL
);


---------------------------------------------------
--11 Person_hasInterest_Tag
---------------------------------------------------
CREATE TABLE Person_hasInterest_Tag (
    creationDate timestamp NOT NULL,
    PersonId bigint NOT NULL,
    TagId bigint NOT NULL
);


---------------------------------------------------
--12 Person_studyAt_University
---------------------------------------------------
CREATE TABLE Person_studyAt_University (
    creationDate timestamp NOT NULL,
    PersonId bigint NOT NULL,
    UniversityId bigint NOT NULL,
    classYear int NOT NULL
);


---------------------------------------------------
--13 Person_workAt_Company
---------------------------------------------------
CREATE TABLE Person_workAt_Company (
    creationDate timestamp NOT NULL,
    PersonId bigint NOT NULL,
    CompanyId bigint NOT NULL,
    workFrom int NOT NULL
);


---------------------------------------------------
--14 Person_knows_Person
---------------------------------------------------
CREATE TABLE Person_knows_Person (
    creationDate timestamp NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL,
    PRIMARY KEY (Person1id, Person2id)
); 


---------------------------------------------------
--15 Message
---------------------------------------------------
CREATE TABLE Message (
    creationDate timestamp NOT NULL,
    id bigint PRIMARY KEY,
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
   
---------------------------------------------------
--16 Person_likes_Message
---------------------------------------------------
CREATE TABLE Person_likes_Message (
    creationDate timestamp NOT NULL,
    PersonId bigint NOT NULL,
    MessageId bigint NOT NULL
);

---------------------------------------------------
--17 Message_hasTag_Tag
---------------------------------------------------
CREATE TABLE Message_hasTag_Tag (
    creationDate timestamp NOT NULL,
    MessageId bigint NOT NULL,
    TagId bigint NOT NULL
);
