SELECT count(*), count(SUBCLASSOFTAGCLASSID)
FROM tagclass;

UPDATE tagclass
SET SUBCLASSOFTAGCLASSID = ID 
WHERE SUBCLASSOFTAGCLASSID IS NULL;

alter table tagclass alter (SUBCLASSOFTAGCLASSID bigint not NULL);
