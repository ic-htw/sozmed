import pandas as pd
import duckdb
con = duckdb.connect(database='sozmed.duckdb', read_only=True)

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

ll = "---------------------------------------------------------------"

print(ll)
print("tables")
print(ll)

sql = """
select table_name, estimated_size from duckdb_tables() order by table_name;
"""
df = con.execute(sql).df()
print(f'{df["estimated_size"].sum():,}')
print(df)

print(ll)
print("constraints")
print(ll)
sql = """
select table_name, constraint_column_names, constraint_type from duckdb_constraints() order by table_name;
"""
df = con.execute(sql).df()
print(df)

print(ll)
print("indexes")
print(ll)
sql = """
select table_name, index_name from duckdb_indexes() order by table_name;
"""
df = con.execute(sql).df()
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
print(df)
