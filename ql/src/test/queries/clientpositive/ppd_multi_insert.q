set hive.optimize.ppd=true;
DROP TABLE mi1;
DROP TABLE mi2;
DROP TABLE mi3;
CREATE TABLE mi1(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE mi2(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE mi3(key INT,ds string,hr string) PARTITION BY list(ds)
subpartition by range(hr)
(
subpartition sp0 values less than("999999"))
(
partition p0 values in ("2008-04-08","2008-04-09")
)
STORED AS TEXTFILE;

EXPLAIN
FROM src a JOIN src b ON (a.key = b.key)
INSERT OVERWRITE TABLE mi1 SELECT a.* WHERE a.key < 100
INSERT OVERWRITE TABLE mi2 SELECT a.key, a.value WHERE a.key >= 100 and a.key < 200
INSERT OVERWRITE TABLE mi3 SELECT a.key ,"2008-04-08",'12' WHERE a.key >= 200 and a.key < 300
INSERT OVERWRITE DIRECTORY '../build/ql/test/data/warehouse/mi4.out' SELECT a.value WHERE a.key >= 300;


FROM src a JOIN src b ON (a.key = b.key)
INSERT OVERWRITE TABLE mi1 SELECT a.* WHERE a.key < 100
INSERT OVERWRITE TABLE mi2 SELECT a.key, a.value WHERE a.key >= 100 and a.key < 200
INSERT OVERWRITE TABLE mi3 SELECT a.key ,"2008-04-08",'12' WHERE a.key >= 200 and a.key < 300
INSERT OVERWRITE DIRECTORY '../build/ql/test/data/warehouse/mi4.out' SELECT a.value WHERE a.key >= 300;

SELECT mi1.* FROM mi1;
SELECT mi2.* FROM mi2;
SELECT mi3.* FROM mi3;
dfs -cat ../build/ql/test/data/warehouse/mi4.out/*;

DROP TABLE mi1;
DROP TABLE mi2;
DROP TABLE mi3;
