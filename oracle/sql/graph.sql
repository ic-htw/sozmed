CREATE PROPERTY GRAPH SOZMED_GRAPH
    VERTEX TABLES (
        Person
        KEY (ID)
        PROPERTIES (ID, Lastname)
    )
    EDGE TABLES (
        Person_knows_Person
        KEY (Person1id, Person2id)
        SOURCE KEY (Person1id) REFERENCES Person(ID)
        DESTINATION KEY (Person2id) REFERENCES Person(ID)
        PROPERTIES (creationDate)
    );


SELECT * FROM GRAPH_TABLE (SOZMED_GRAPH
  MATCH
  (p1 IS person WHERE p1.id=1989) -[k IS Person_knows_Person]->(p2 IS person)
  COLUMNS (p1.id AS id1, p1.lastname AS ln1, p2.id AS id2, p2.lastname AS ln2)
);