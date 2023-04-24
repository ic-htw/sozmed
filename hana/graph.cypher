select * 
from OPENCYPHER_TABLE( GRAPH WORKSPACE "SOZMED"."GWS_SOZMED" QUERY
  'match (tc:TagClass) return tc.NAME as Name'
);

select * 
from OPENCYPHER_TABLE( GRAPH WORKSPACE "SOZMED"."GWS_SOZMED" QUERY
  '
  match (t:Tags) -[r]-> (tc:TagClass) 
  return t.NAME as Tag, tc.NAME as TagClass
  '
);