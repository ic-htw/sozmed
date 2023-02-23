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
      CREATE (x:Message {{id:r.id, length:r.length, ParentMessageId:r.ParentMessageId}})
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT c_Message FOR (x:Message) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    # exe(driver, q3, q4)  

    # -------------------------------------------------
    # 09a Post, Comment
    # -------------------------------------------------
    q1 = f"""
    MATCH (x:Message) where x.ParentMessageId is null set x:Post;
    """
    q2 = f"""
    MATCH (x:Message) where x.ParentMessageId is not null set x:Comment;
    """

    # exe(driver, q1, q2)  

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
