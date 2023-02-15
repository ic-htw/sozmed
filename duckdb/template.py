import duckdb
con = duckdb.connect(database='socmed.duckdb', read_only=False)

sql = """
xxx
"""
con.execute(sql)

print("ok")


# -------------------------------------------------
# xxx
# -------------------------------------------------
sql1 = """
drop table if exists xxx;
"""

sql2 = """
xxx
"""

sql3 = """
insert into xxx
  select * from read_csv(f'{csvdir}/xxx.csv', delim='|', header=True, AUTO_DETECT=TRUE);
"""

con.execute(sql1)
con.execute(sql2)
con.execute(sql3)
