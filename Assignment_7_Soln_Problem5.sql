-----------problem 5----


---------------------------------
--------------------------------
---------------------------------
drop table if exists graph;
create table graph(source integer,target integer);
insert into graph values(1,2);
insert into graph values(1,3);
insert into graph values(1,4);
insert into graph values(2,3);
insert into graph values(2,4);
insert into graph values(3,1);
insert into graph values(3,4);
insert into graph values(4,4);


select * from graph;

drop function if exists data_flatten();
create or replace function data_flatten() returns void as 
$$
declare dyn text;
	num_nodes integer;
	i integer;
	temp_node integer;
begin
	select count(distinct a.nodes) into num_nodes from (select distinct source nodes from graph union select distinct target nodes from graph)a;
	drop table if exists data_flattened;
	create table data_flattened (source integer, target integer,weight integer);
	drop table if exists data_flattened_transpose;
	create table data_flattened_transpose (source integer, target integer,weight integer);
	for i in 1..num_nodes loop
	
		select distinct b.nodes into temp_node from(select distinct source nodes from graph union select distinct target nodes from graph)b order by nodes limit 1 offset i-1;
		insert into data_flattened(
		 select temp_node,b.nodes,case when a.target is null then 0 else 1 end
		 from (select a.target from graph  a where a.source=temp_node)a
		right outer join 
		(select distinct b.nodes,1 val from(select distinct source nodes from graph union select distinct target nodes from graph)b order by nodes)b 
		on a.target=b.nodes
		order by b.nodes);

		insert into data_flattened_transpose(
		 select b.nodes,temp_node,case when a.target is null then 0 else 1 end
		 from (select a.target from graph  a where a.source=temp_node)a
		right outer join 
		(select distinct b.nodes,1 val from(select distinct source nodes from graph union select distinct target nodes from graph)b order by nodes)b 
		on a.target=b.nodes
		order by b.nodes);
		
	end loop;
	
end;
$$ language plpgsql;

drop function if exists hits(float);
create or replace function hits(threshold float) returns void as
$$
declare num_nodes integer;
	x record;
	error float;
	error_hub float;
	error_authority float;
begin
	error = 99999;
	perform data_flatten();
	drop table if exists hub;
	create table hub(source integer,target integer,weight float);

	drop table if exists authority;
	create table authority(source integer,target integer,weight float);
	insert into authority(select a.source, 0 as target, 1 as weight from (select distinct source from data_flattened union select distinct source from data_flattened_transpose)a);

	insert into hub(select a.source, 0 as target, 1 as weight from (select distinct source from data_flattened union select distinct source from data_flattened_transpose)a);


	drop table if exists temp_flatten ;
	create table temp_flatten as (SELECT A.source,
	   B.target,
	   SUM(A.weight * B.weight) AS weight
	FROM data_flattened A
	INNER JOIN data_flattened_transpose B
	ON A.target = B.source
	GROUP BY A.source, B.target
	order by A.source,
	   B.target);

	while error>threshold loop
		drop table if exists hub_current ;
		create table hub_current(source integer, target integer,weight float);
		drop table if exists authority_current;
		create table authority_current (source integer, target integer,weight float);

			insert into authority_current 
			(SELECT A.source,B.target, SUM(A.weight * B.weight) AS value
				FROM data_flattened_transpose A
				INNER JOIN hub B
				ON A.target = B.source
				GROUP BY A.source, B.target
				order by A.source,
				B.target);

		perform normalize_hub(3);
				
			insert into hub_current 
			(SELECT A.source,B.target, SUM(A.weight * B.weight) AS value
				FROM temp_flatten A
				INNER JOIN hub B
				ON A.target = B.source
				GROUP BY A.source, B.target
				order by A.source,
				B.target);

			
		
		perform normalize_hub(2);

		select sum(c.weight) into error_hub from(select (a.weight-b.weight) as weight from
		(select source,target,weight from hub)a,
		(select source,target,weight from hub_current)b
		where a.source = b.source and a.target=b.target)c;

		select sum(c.weight) into error_authority from(select (a.weight-b.weight) as weight from
		(select source,target,weight from authority)a,
		(select source,target,weight from authority_current)b
		where a.source = b.source and a.target=b.target)c;

		select error_hub+error_authority into error;
		
		delete from hub;
		insert into hub(select * from hub_current);
		delete from authority;
		insert into authority(select * from authority_current);
	end loop;

end;
$$ language plpgsql;


drop function if exists normalize_hub(int);
create or replace function normalize_hub(flag integer) returns void as 
$$
declare normalize_factor float;
	b record;
begin
drop table if exists temp_table;
if flag = 1 then
	create table temp_table as (select a.source,0 as taget,a.weight/e.weight weight from
			(select source,weight from hub)a,
			(select sqrt(sum(weight*weight)) weight  from hub)e);
	delete from hub;
	insert into hub(select * from temp_table);
end if;
if flag=2 then
	create table temp_table as (select a.source,0 as target,a.weight/e.weight weight from
			(select source,weight from hub_current)a,
			(select sqrt(sum(weight*weight)) weight  from hub_current)e);
	delete from hub_current;
	insert into hub_current(select * from temp_table);
end if;
if flag=3 then 
	create table temp_table as (select a.source,0 as target,a.weight/e.weight weight from
			(select source,weight from authority_current)a,
			(select sqrt(sum(weight*weight)) weight  from authority_current)e);
	delete from authority_current;
	insert into authority_current(select * from temp_table);
end if;
end;
$$ language plpgsql;

select hits(0.0000001);

select * from authority;
select * from hub;