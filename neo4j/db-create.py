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
      CREATE (x:Message {{id:r.id, length:r.length, parentmessageid:r.parentmessageid}})
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT c_Message FOR (x:Message) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    exe(driver, q3, q4)  

    # -------------------------------------------------
    # xxx
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT xxx;
    """
    q2 = f"""
    MATCH (x:xxx) DETACH DELETE x;
    """

    q3 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/xxx.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MERGE (x:xxx {{id:r.id}})
        ON CREATE SET x.name = r.name
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT xxx FOR (x:xxx) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    # exe(driver, q3, q4)  

    # -------------------------------------------------
    # xxx
    # -------------------------------------------------
    q1 = f"""
    DROP CONSTRAINT xxx;
    """
    q2 = f"""
    MATCH (x:xxx) DETACH DELETE x;
    """

    q3 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/xxx.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MERGE (x:xxx {{id:r.id}})
        ON CREATE SET x.name = r.name
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    q4 = f"""
    CREATE CONSTRAINT xxx FOR (x:xxx) REQUIRE x.id IS UNIQUE;
    """

    # exe(driver, q1, q2)  
    # exe(driver, q3, q4)  



# -------------------------------------------------
print("ok")
# -------------------------------------------------
