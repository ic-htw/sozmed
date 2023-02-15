import duckdb
con = duckdb.connect(database='socmed.duckdb', read_only=True)

sql = """
select table_name, estimated_size from duckdb_tables() order by table_name;
"""
df = con.execute(sql).df()
print(f'{df["estimated_size"].sum():,}')
print(df)