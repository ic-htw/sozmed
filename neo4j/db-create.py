from neo4j import GraphDatabase
import cred as c

def exe(driver, *qs):
    with driver.session() as session:
        for q in qs:
          session.run(q)

with GraphDatabase.driver(c.neo4j_host, auth=(c.neo4j_userid, c.neo4j_password)) as driver:

    # -------------------------------------------------
    # 01 TagClass
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT c_tagclass;
    """
    
    q2 = f"""
    MATCH (x:TagClass) DETACH DELETE x;
    """

    q3 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/TagClass.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      CREATE (x:TagClass {{id:r.id, name:r.name}})
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT c_tagclass FOR (x:TagClass) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    # exe(driver, q3, q4)  

    # -------------------------------------------------
    # 02 Tag
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT c_tag;
    """
    q2 = f"""
    MATCH (x:Tag) DETACH DELETE x;
    """

    q3 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Tag.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      CREATE (x:Tag {{id:r.id, name:r.name}})
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT c_tag FOR (x:Tag) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    # exe(driver, q3, q4)  
 
    # -------------------------------------------------
    # 03 Country
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT c_Country;
    """
    q2 = f"""
    MATCH (x:Country) DETACH DELETE x;
    """

    q3 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Country.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      CREATE (x:Country {{id:r.id, name:r.name}})
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT c_Country FOR (x:Country) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    # exe(driver, q3, q4)  

    # -------------------------------------------------
    # 04 City
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT c_City;
    """
    q2 = f"""
    MATCH (x:City) DETACH DELETE x;
    """

    q3 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/City.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      CREATE (x:City {{id:r.id, name:r.name}})
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT c_City FOR (x:City) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    # exe(driver, q3, q4)  

    # -------------------------------------------------
    # 05 Company
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT c_Company;
    """
    q2 = f"""
    MATCH (x:Company) DETACH DELETE x;
    """

    q3 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Company.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      CREATE (x:Company {{id:r.id, name:r.name}})
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT c_Company FOR (x:Company) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    # exe(driver, q3, q4)  

    # -------------------------------------------------
    # 06 University
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT c_University;
    """
    q2 = f"""
    MATCH (x:University) DETACH DELETE x;
    """

    q3 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/University.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      CREATE (x:University {{id:r.id, name:r.name}})
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT c_University FOR (x:University) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    # exe(driver, q3, q4)  

    # -------------------------------------------------
    # 07 Person
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT c_Person;
    """
    q2 = f"""
    MATCH (x:Person) DETACH DELETE x;
    """

    q3 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Person.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      CREATE (x:Person {{id:r.id, name:r.name}})
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT c_Person FOR (x:Person) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    # exe(driver, q3, q4)  

    # -------------------------------------------------
    # 08 Forum
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT c_Forum;
    """
    q2 = f"""
    MATCH (x:Forum) DETACH DELETE x;
    """

    q3 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Forum.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      CREATE (x:Forum {{id:r.id, name:r.name}})
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT c_Forum FOR (x:Forum) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    # exe(driver, q3, q4)  

    # -------------------------------------------------
    # 09 Message
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT c_Message;
    """
    q2 = f"""
    call apoc.periodic.iterate(
      'MATCH (x:Message) return x', 
      'DETACH DELETE x', 
      {{batchSize:100000, batchMode:"BATCH"}}
    )
    """

    q3 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Message.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      CREATE (x:Message {{
        id:r.id, 
        length:r.length,
        ParentMessageId:r.ParentMessageId}})
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT c_Message FOR (x:Message) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    # exe(driver, q3, q4)  

    # -------------------------------------------------
    # 09a Post
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT c_Post;
    """
    q2 = f"""
    MATCH (x:Message) where x.ParentMessageId is null set x:Post;
    """
    q3 = f"""
    CREATE CONSTRAINT c_Post FOR (x:Post) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2, q3)  

    # -------------------------------------------------
    # 09b Comment
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT c_Comment;
    """
    q2 = f"""
    MATCH (x:Message) where x.ParentMessageId is not null set x:Comment;
    """
    q3 = f"""
    CREATE CONSTRAINT c_Comment FOR (x:Comment) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2, q3)  


    # -------------------------------------------------
    # HAS_TYPE
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Tag)-[z:HAS_TYPE]->(y:TagClass) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/zzz.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Tag {{id: r.id}})
      MATCH (y:TagClass {{id: r.TypeTagClassId}})
      CREATE (x)-[:HAS_TYPE]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  

    # -------------------------------------------------
    # IS_PART_OF
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:City)-[z:IS_PART_OF]->(y:Country) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/City.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:City {{id: r.id}})
      MATCH (y:Country {{id: r.PartOfCountryId}})
      CREATE (x)-[:IS_PART_OF]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  


    # -------------------------------------------------
    # IS_LOCATED_IN
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Company)-[z:IS_LOCATED_IN]->(y:Country) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Company.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Company {{id: r.id}})
      MATCH (y:Country {{id: r.LocationPlaceId}})
      CREATE (x)-[:IS_LOCATED_IN]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  


    # -------------------------------------------------
    # IS_LOCATED_IN
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:University)-[z:IS_LOCATED_IN]->(y:City) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/University.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:University {{id: r.id}})
      MATCH (y:City {{id: r.LocationPlaceId}})
      CREATE (x)-[:IS_LOCATED_IN]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  


    # -------------------------------------------------
    # IS_LOCATED_IN
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Person)-[z:IS_LOCATED_IN]->(y:City) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Person.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Person {{id: r.id}})
      MATCH (y:City {{id: r.LocationCityId}})
      CREATE (x)-[:IS_LOCATED_IN]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  


    # -------------------------------------------------
    # IS_LOCATED_IN
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Message)-[z:IS_LOCATED_IN]->(y:Country) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Message.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Message {{id: r.id}})
      MATCH (y:Country {{id: r.LocationCountryId}})
      CREATE (x)-[:IS_LOCATED_IN]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  


    # -------------------------------------------------
    # STUDY_AT
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Person)-[z:STUDY_AT]->(y:University) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Person_studyAt_University.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Person {{id: r.PersonId}})
      MATCH (y:University {{id: r.UniversityId}})
      CREATE (x)-[:STUDY_AT]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  


    # -------------------------------------------------
    # WORK_AT
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Person)-[z:WORK_AT]->(y:Company) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Person_workAt_Company.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Person {{id: r.PersonId}})
      MATCH (y:Company {{id: r.CompanyId}})
      CREATE (x)-[:WORK_AT]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  


    # -------------------------------------------------
    # KNOWS
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Person)-[z:KNOWS]->(y:Person) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Person_knows_Person.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Person {{id: r.Person1Id}})
      MATCH (y:Person {{id: r.Person2Id}})
      CREATE (x)-[:KNOWS]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  


    # -------------------------------------------------
    # HAS_INTEREST
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Person)-[z:HAS_INTEREST]->(y:Tag) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Person_hasInterest_Tag.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Person {{id: r.PersonId}})
      MATCH (y:Tag {{id: r.TagId}})
      CREATE (x)-[:HAS_INTEREST]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  

    # -------------------------------------------------
    # LIKES
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Person)-[z:LIKES]->(y:Message) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Person_likes_Message.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Person {{id: r.PersonId}})
      MATCH (y:Message {{id: r.id}})
      CREATE (x)-[:LIKES]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  



    # -------------------------------------------------
    # HAS_MEMBER
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Forum)-[z:HAS_MEMBER]->(y:Person) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Forum_hasMember_Person.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Forum {{id: r.ForumId}})
      MATCH (y:Person {{id: r.PersonId}})
      CREATE (x)-[:HAS_MEMBER]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  

    # -------------------------------------------------
    # HAS_TAG
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Forum)-[z:HAS_TAG]->(y:Tag) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Forum_hasTag_Tag.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Forum {{id: r.ForumId}})
      MATCH (y:Tag {{id: r.TagId}})
      CREATE (x)-[:HAS_TAG]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  

    # -------------------------------------------------
    # HAS_MODERATOR
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Forum)-[z:HAS_MODERATOR]->(y:Person) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Forum.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Forum {{id: r.id}})
      MATCH (y:Person {{id: r.ModeratorPersonId}})
      CREATE (x)-[:HAS_MODERATOR]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  

    # -------------------------------------------------
    # CONTAINER_OF
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Forum)-[z:CONTAINER_OF]->(y:Post) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Message.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Forum {{id: r.ContainerForumId}})
      MATCH (y:Post {{id: r.id}})
      CREATE (x)-[:CONTAINER_OF]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    exe(driver, q1, q2)  

    # -------------------------------------------------
    # HAS_CREATOR
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Message)-[z:HAS_CREATOR]->(y:Person) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Message.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:Message {{id: r.id}})
      MATCH (y:Person {{id: r.CreatorPersonId}})
      CREATE (x)-[:HAS_CREATOR]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  

    # -------------------------------------------------
    # zzz
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:xxx)-[z:zzz]->(y:yyy) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/zzz.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:xxx {{id: r.iii}})
      MATCH (y:yyy {{id: r.iii}})
      CREATE (x)-[:zzz]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  

    # -------------------------------------------------
    # zzz
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:xxx)-[z:zzz]->(y:yyy) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/zzz.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:xxx {{id: r.iii}})
      MATCH (y:yyy {{id: r.iii}})
      CREATE (x)-[:zzz]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  

    # -------------------------------------------------
    # zzz
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:xxx)-[z:zzz]->(y:yyy) DETACH DELETE z;
    """

    q2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/zzz.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MATCH (x:xxx {{id: r.iii}})
      MATCH (y:yyy {{id: r.iii}})
      CREATE (x)-[:zzz]->(y)
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    # exe(driver, q1, q2)  




# -------------------------------------------------
print("ok")
# -------------------------------------------------
