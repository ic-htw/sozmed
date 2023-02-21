from neo4j import GraphDatabase
import cred as c

with GraphDatabase.driver(c.neo4j_host, auth=(c.neo4j_userid, c.neo4j_password)) as driver:

    # -------------------------------------------------
    # 01 TagClass
    # -------------------------------------------------
    cypher1 = f"""
    DROP CONSTRAINT c_tagclass;
    """
    
    cypher2 = f"""
    MATCH (x:TagClass) DETACH DELETE x;
    """

    cypher3 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/TagClass.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MERGE (x:TagClass {{id:r.id}})
        ON CREATE SET x.name = r.name
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    cypher4 = f"""
    CREATE CONSTRAINT c_tagclass FOR (x:TagClass) REQUIRE x.id IS UNIQUE;
    """

    with driver.session() as session:
        session.run(cypher1)
        session.run(cypher2)
        session.run(cypher3)
        session.run(cypher4)


    # -------------------------------------------------
    # 02 Tag
    # -------------------------------------------------
    cypher1 = f"""
    DROP CONSTRAINT constraint_78b13592;
    MATCH (x:Tag) DETACH DELETE x;
    """

    cypher2 = f"""
    LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Tag.csv' AS r FIELDTERMINATOR '|'
    CALL {{
      WITH r
      MERGE (x:Tag {{id:r.id}})
        ON CREATE SET x.name = r.name
    }} IN TRANSACTIONS OF 100000 ROWS;
    """

    cypher3 = f"""
    CREATE CONSTRAINT c_tag FOR (x:Tag) REQUIRE x.id IS UNIQUE;
    """

    # with driver.session() as session:
    #     session.run(cypher1)
        # session.run(cypher2)
        # session.run(cypher3)

# -------------------------------------------------
print("ok")
# -------------------------------------------------
