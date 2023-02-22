
import pandas as pd
from neo4j import GraphDatabase
import cred as c

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

with GraphDatabase.driver(c.neo4j_host, auth=(c.neo4j_userid, c.neo4j_password)) as driver:
    q = f"""
    MATCH (x:Forum) return count(*);
    """
    with driver.session() as session:
        rs = session.run(q)
        df = pd.DataFrame(rs.data())
    print(df)