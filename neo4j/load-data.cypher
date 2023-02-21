
# ----------------------------------------------------------------------------
# Person
# ----------------------------------------------------------------------------

MATCH (p:Person) DETACH DELETE p;

:auto LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Person.csv' AS r FIELDTERMINATOR '|'
CALL {
  WITH r
  MERGE (p:Person {id:r.id})
    ON CREATE SET p.lastName = r.lastName
} IN TRANSACTIONS OF 100000 ROWS;

CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE;


# ----------------------------------------------------------------------------
# KNOWS
# ----------------------------------------------------------------------------
:auto LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Person_knows_Person.csv' AS r FIELDTERMINATOR '|'
CALL {
  WITH r
  MATCH (p1:Person {id: r.Person1Id})
  MATCH (p2:Person {id: r.Person2Id})
  MERGE (p1)-[:KNOWS]->(p2)
} IN TRANSACTIONS OF 100000 ROWS;



# ----------------------------------------------------------------------------
# Tag
# ----------------------------------------------------------------------------

MATCH (t:Tag) DETACH DELETE t;

:auto LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Tag.csv' AS r FIELDTERMINATOR '|'
CALL {
  WITH r
  MERGE (t:Tag {id:r.id})
    ON CREATE SET t.name = r.name
} IN TRANSACTIONS OF 100000 ROWS;

CREATE CONSTRAINT FOR (t:Tag) REQUIRE t.id IS UNIQUE;

# ----------------------------------------------------------------------------
# HAS_INTEREST
# ----------------------------------------------------------------------------
:auto LOAD CSV WITH HEADERS FROM 'file:///csv/sozmed/Person_hasInterest_Tag.csv' AS r FIELDTERMINATOR '|'
CALL {
  WITH r
  MATCH (p:Person {id: r.PersonId})
  MATCH (t:Tag {id: r.TagId})
  MERGE (p)-[:HAS_INTEREST]->(t)
} IN TRANSACTIONS OF 100000 ROWS;



