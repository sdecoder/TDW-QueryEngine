drop database db1;
drop database db2;
create database db1;
create database db2;

use db1;
create table kv(key string,value string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

load data local inpath '../data/files/db1_kv.txt' into table kv;

select * from kv;

use db2;

create table kv(key string,value string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

load data local inpath '../data/files/db2_kv.txt' into table kv;

select * from kv;

explain 
select * from db1::kv join db2::kv on(db1::kv.key=db2::kv.key);

select * from db1::kv join db2::kv on(db1::kv.key=db2::kv.key);

explain
select b.value from db1::kv a join db2::kv b on(a.key=db2::kv.key);

select b.value from db1::kv a join db2::kv b on(a.key=db2::kv.key);

explain 
select count(kv.value) from kv group by db2::kv.key;

select count(kv.value) from kv group by db2::kv.key;

explain select a.key,b.value from db1::kv a join db2::kv b on(a.key=b.key);
select a.key,b.value from db1::kv a join db2::kv b on(a.key=b.key);

explain select a.key,b.value from kv a join kv b on(a.key=b.key);
select a.key,b.value from kv a join kv b on(a.key=b.key);
drop table kv;
use db1;
drop table kv;
use default_db;
drop database db1;
drop database db2;
