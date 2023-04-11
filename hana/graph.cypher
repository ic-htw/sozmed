select * 
from OPENCYPHER_TABLE( GRAPH WORKSPACE "SOZMED"."GWS_SOZMED" QUERY
  'match (tc:TagClass) return tc.NAME as aaa'
);