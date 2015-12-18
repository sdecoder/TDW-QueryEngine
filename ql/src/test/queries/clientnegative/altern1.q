drop table altern1;
create table altern1(a int, b int, ds string) partition by list(ds)(
partition p1 values in('x','6')
);
alter table altern1 replace columns(a int, b int, ds string);
drop table altern1;
