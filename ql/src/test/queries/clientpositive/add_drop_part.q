show partitions srcpart;

select * from srcpart;

alter table srcpart add partition addp1 values in ('2010-01-01','2010-01-02'),
			partition addp2 values in ('2010-02-01','2010-02-02','2010-02-03');

show partitions srcpart;

alter table srcpart add subpartition addsp1 values less than('990'),
			subpartition addsp2 values less than ('9990');
show partitions srcpart;


alter table srcpart add default partition;

show partitions srcpart;

alter table srcpart add default subpartition;

show partitions srcpart;

alter table srcpart drop partition(addp1),partition(addp2);

show partitions srcpart;

alter table srcpart drop subpartition(addsp1),subpartition(addsp2);

show partitions srcpart;

alter table srcpart drop partition(default);

show partitions srcpart;

alter table srcpart drop subpartition(default);
 
show partitions srcpart;

select * from srcpart;
