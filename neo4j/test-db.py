
import pandas as pd
from neo4j import GraphDatabase
import cred as c

with GraphDatabase.driver(c.neo4j_host, auth=(c.neo4j_userid, c.neo4j_password)) as driver:
    cypher = f"""
    MATCH (x:TagClass) return count(*);
    """
    with driver.session() as session:
        rs = session.run(cypher)
        df = pd.DataFrame(rs.data())
    print(df)