set hive.mapred.mode=strict;

select * from 
  (select count(1) as cnt from src 
    union all
   select count(1) as cnt from srcpart where ds = '2009-08-09'
  )x sort by x.cnt;
   

select * from 
  (select * from src 
    union all
   select * from srcpart where ds = '2009-08-09'
  )x sort by x.key;
