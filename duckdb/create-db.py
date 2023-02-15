import duckdb
con = duckdb.connect(database='sozmed.duckdb', read_only=False)
csvdir = '../csv'

# -------------------------------------------------
# TagClass
# -------------------------------------------------
sql1 = """
drop table if exists TagClass;
"""

sql2 = """
CREATE TABLE TagClass (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    url text NOT NULL,
    SubclassOfTagClassId bigint -- null for the root TagClass (Thing)
);
"""

sql3 = """
insert into TagClass
  select * from read_csv(f'{csvdir}/TagClass.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# Tag
# -------------------------------------------------
sql1 = """
drop table if exists xxx;
"""

sql2 = """
CREATE TABLE Tag (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    url text NOT NULL,
    TypeTagClassId bigint NOT NULL
);
"""

sql3 = """
insert into Tag
  select * from read_csv(f'{csvdir}/Tag.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# place
# -------------------------------------------------
sql1 = """
drop table if exists place;
"""

sql2 = """
CREATE TABLE Place (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    url text NOT NULL,
    type text NOT NULL,
    PartOfPlaceId bigint -- null for continents
)
"""

sql3 = """
insert into place
  select * from read_csv(f'{csvdir}/Place.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)

# -------------------------------------------------
# Organisation
# -------------------------------------------------
sql1 = """
drop table if exists Organisation;
"""

sql2 = """
CREATE TABLE Organisation (
    id bigint PRIMARY KEY,
    type text NOT NULL,
    name text NOT NULL,
    url text NOT NULL,
    LocationPlaceId bigint NOT NULL
);
"""

sql3 = """
insert into Organisation
  select * from read_csv(f'{csvdir}/Organisation.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)



# -------------------------------------------------
# Person
# -------------------------------------------------
sql1 = """
drop table if exists Person;
"""

sql2 = """
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

sql3 = """
insert into Person
  select * from read_csv(f'{csvdir}/Person1.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Person
  select * from read_csv(f'{csvdir}/Person2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)

# -------------------------------------------------
# Post
# -------------------------------------------------
sql1 = """
drop table if exists Post;
"""

sql2 = """
CREATE TABLE Post (
    creationDate timestamp with time zone NOT NULL,
    id bigint NOT NULL, --PRIMARY KEY,
    imageFile text,
    locationIP text NOT NULL,
    browserUsed text NOT NULL,
    language text,
    content text,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    ContainerForumId bigint NOT NULL,
    LocationCountryId bigint NOT NULL
);
"""

sql3 = """
insert into Post
  select * from read_csv(f'{csvdir}/Post2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Post
  select * from read_csv(f'{csvdir}/Post3.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# Comment
# -------------------------------------------------
sql1 = """
drop table if exists Comment;
"""

sql2 = """
CREATE TABLE Comment (
    creationDate timestamp with time zone NOT NULL,
    id bigint NOT NULL, --PRIMARY KEY,
    locationIP text NOT NULL,
    browserUsed text NOT NULL,
    content text NOT NULL,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    LocationCountryId bigint NOT NULL,
    ParentPostId bigint,
    ParentCommentId bigint
);
"""

sql3 = """
insert into Comment
  select * from read_csv(f'{csvdir}/Comment2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Comment
  select * from read_csv(f'{csvdir}/Comment3.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# Forum
# -------------------------------------------------
sql1 = """
drop table if exists Forum;
"""

sql2 = """
CREATE TABLE Forum (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    title text NOT NULL,
    ModeratorPersonId bigint -- can be null as its cardinality is 0..1
);
"""

sql3 = """
insert into Forum
  select * from read_csv(f'{csvdir}/Forum1.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Forum
  select * from read_csv(f'{csvdir}/Forum2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# Comment_hasTag_Tag
# -------------------------------------------------
sql1 = """
drop table if exists Comment_hasTag_Tag;
"""

sql2 = """
CREATE TABLE Comment_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    CommentId bigint NOT NULL,
    TagId bigint NOT NULL
);
"""

sql3 = """
insert into Comment_hasTag_Tag
  select * from read_csv(f'{csvdir}/Comment_hasTag_Tag2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Comment_hasTag_Tag
  select * from read_csv(f'{csvdir}/Comment_hasTag_Tag3.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# Post_hasTag_Tag
# -------------------------------------------------
sql1 = """
drop table if exists Post_hasTag_Tag;
"""

sql2 = """
CREATE TABLE Post_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    PostId bigint NOT NULL,
    TagId bigint NOT NULL
);
"""

sql3 = """
insert into Post_hasTag_Tag
  select * from read_csv(f'{csvdir}/Post_hasTag_Tag1.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Post_hasTag_Tag
  select * from read_csv(f'{csvdir}/Post_hasTag_Tag2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# Forum_hasMember_Person
# -------------------------------------------------
sql1 = """
drop table if exists Forum_hasMember_Person;
"""

sql2 = """
CREATE TABLE Forum_hasMember_Person (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
);
"""

sql3 = """
insert into Forum_hasMember_Person
  select * from read_csv(f'{csvdir}/Forum_hasMember_Person2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Forum_hasMember_Person
  select * from read_csv(f'{csvdir}/Forum_hasMember_Person3.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# Forum_hasTag_Tag
# -------------------------------------------------
sql1 = """
drop table if exists Forum_hasTag_Tag;
"""

sql2 = """
CREATE TABLE Forum_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    TagId bigint NOT NULL
);
"""

sql3 = """
insert into Forum_hasTag_Tag
  select * from read_csv(f'{csvdir}/Forum_hasTag_Tag1.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Forum_hasTag_Tag
  select * from read_csv(f'{csvdir}/Forum_hasTag_Tag2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)



# -------------------------------------------------
# Person_hasInterest_Tag
# -------------------------------------------------
sql1 = """
drop table if exists Person_hasInterest_Tag;
"""

sql2 = """
CREATE TABLE Person_hasInterest_Tag (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    TagId bigint NOT NULL
);
"""

sql3 = """
insert into Person_hasInterest_Tag
  select * from read_csv(f'{csvdir}/Person_hasInterest_Tag1.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Person_hasInterest_Tag
  select * from read_csv(f'{csvdir}/Person_hasInterest_Tag2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# Person_likes_Comment
# -------------------------------------------------
sql1 = """
drop table if exists Person_likes_Comment;
"""

sql2 = """
CREATE TABLE Person_likes_Comment (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
);
"""

sql3 = """
insert into Person_likes_Comment
  select * from read_csv(f'{csvdir}/Person_likes_Comment2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Person_likes_Comment
  select * from read_csv(f'{csvdir}/Person_likes_Comment3.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# Person_likes_Post
# -------------------------------------------------
sql1 = """
drop table if exists Person_likes_Post;
"""

sql2 = """
CREATE TABLE Person_likes_Post (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
);
"""

sql3 = """
insert into Person_likes_Post
  select * from read_csv(f'{csvdir}/Person_likes_Post1.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Person_likes_Post
  select * from read_csv(f'{csvdir}/Person_likes_Post2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# Person_studyAt_University
# -------------------------------------------------
sql1 = """
drop table if exists Person_studyAt_University;
"""

sql2 = """
CREATE TABLE Person_studyAt_University (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    UniversityId bigint NOT NULL,
    classYear int NOT NULL
);
"""

sql3 = """
insert into Person_studyAt_University
  select * from read_csv(f'{csvdir}/Person_studyAt_University1.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Person_studyAt_University
  select * from read_csv(f'{csvdir}/Person_studyAt_University2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# Person_workAt_Company
# -------------------------------------------------
sql1 = """
drop table if exists Person_workAt_Company;
"""

sql2 = """
CREATE TABLE Person_workAt_Company (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CompanyId bigint NOT NULL,
    workFrom int NOT NULL
);
"""

sql3 = """
insert into Person_workAt_Company
  select * from read_csv(f'{csvdir}/Person_workAt_Company1.csv', delim='|', header=True, AUTO_DETECT=TRUE);
insert into Person_workAt_Company
  select * from read_csv(f'{csvdir}/Person_workAt_Company2.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)


# -------------------------------------------------
# Person_knows_Person
# -------------------------------------------------
sql1 = """
drop table if exists Person_knows_Person;
"""

sql2 = """
CREATE TABLE Person_knows_Person (
    creationDate timestamp with time zone NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL,
    PRIMARY KEY (Person1id, Person2id)
); 
"""

sql3 = """
insert into Person_knows_Person
  select * from read_csv(f'{csvdir}/Person_knows_Person1.csv', delim='|', header=True, AUTO_DETECT=TRUE, SAMPLE_SIZE=-1);
insert into Person_knows_Person
  select * from read_csv(f'{csvdir}/Person_knows_Person2.csv', delim='|', header=True, AUTO_DETECT=TRUE, SAMPLE_SIZE=-1);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)



# -------------------------------------------------
# xxx
# -------------------------------------------------
sql1 = """
drop table if exists xxx;
"""

sql2 = """
xxx
"""

sql3 = """
insert into xxx
  select * from read_csv(f'{csvdir}/xxx.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

# con.execute(sql1)
# con.execute(sql2)
# con.execute(sql3)

# -------------------------------------------------
# xxx
# -------------------------------------------------
sql1 = """
drop table if exists xxx;
"""

sql2 = """
xxx
"""

sql3 = """
insert into xxx
  select * from read_csv(f'{csvdir}/xxx.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

# con.execute(sql1)
# con.execute(sql2)
# con.execute(sql3)

# -------------------------------------------------
# xxx
# -------------------------------------------------
sql1 = """
drop table if exists xxx;
"""

sql2 = """
xxx
"""

sql3 = """
insert into xxx
  select * from read_csv(f'{csvdir}/xxx.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

# con.execute(sql1)
# con.execute(sql2)
# con.execute(sql3)

# -------------------------------------------------
# xxx
# -------------------------------------------------
sql1 = """
drop table if exists xxx;
"""

sql2 = """
xxx
"""

sql3 = """
insert into xxx
  select * from read_csv(f'{csvdir}/xxx.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

# con.execute(sql1)
# con.execute(sql2)
# con.execute(sql3)

# -------------------------------------------------
print("ok")
# -------------------------------------------------


