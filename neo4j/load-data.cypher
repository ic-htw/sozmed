
# ----------------------------------------------------------------------------
# Person
# ----------------------------------------------------------------------------

MATCH (p:Person) DETACH DELETE p;

:auto LOAD CSV WITH HEADERS FROM 'file:///sozmed/Person.csv' AS r FIELDTERMINATOR '|'
CALL {
  WITH r
  CREATE (p:Person {id:r.id, lastName:r.lastName, creationDate:datetime(replace(r.creationDate, ' ', 'T'))})
} IN TRANSACTIONS OF 100000 ROWS;

CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE;

MATCH (p:Person) where p.id='14' return p.creationDate + duration({hours: 4});



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


