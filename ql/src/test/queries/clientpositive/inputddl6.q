-- test for describe extended table
-- test for describe extended table partition
-- test for alter table drop partition
DROP TABLE INPUTDDL6;
CREATE TABLE INPUTDDL6(KEY STRING, VALUE STRING, ds STRING) PARTITION BY list(ds)
(PARTITION p0 VALUES IN ('2008-04-09'),
PARTITION p1 VALUES IN ('2008-04-08')) 
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE INPUTDDL6 SELECT key, value, '2008-04-09' FROM src;
INSERT OVERWRITE TABLE INPUTDDL6 SELECT key, value, '2008-04-08' FROM src;
DESCRIBE EXTENDED INPUTDDL6;
SHOW PARTITIONS INPUTDDL6;
ALTER TABLE INPUTDDL6 DROP PARTITION (p0);
SHOW PARTITIONS INPUTDDL6;
DROP TABLE INPUTDDL6;

