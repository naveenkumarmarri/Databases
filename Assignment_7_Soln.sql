drop table if exists E;
create table E(source integer,target integer);
insert into E values(1,2);
insert into E values(1,3);
insert into E values(2,3);
insert into E values(3,2);
insert into E values(3,4);
insert into E values(3,5);
insert into E values(6,7);
insert into E values(6,6);
insert into E values(7,8);
insert into E values(4,9);
insert into E values(9,5);

create table if not exists TC(source integer, target integer);
create or replace function new_TC_pairs()
returns table (source integer, target integer) AS
$$
	(select TC.source, E.target
	from TC, E
	where TC.target = E.source)
	except
	(select source, target
	from TC);
$$ LANGUAGE SQL;

create or replace function Transitive_Closure()
returns void as $$
begin
	drop table if exists TC;
	create table TC(source integer, target integer);
	insert into TC select * from E;
	while exists(select * from new_TC_pairs())
	loop
		insert into TC select * from new_TC_pairs();
	end loop;
end;
$$ language plpgsql;


select Transitive_Closure();
select * from tc;
----------------------------------------
----------------------------------------
drop table if exists TC_temp;
create table TC_temp(source integer, target integer);
drop table if exists E_temp;
create table E_temp(source integer,target integer);
insert into E_temp(select * from (select * from E where target <>6)a where a.source<>6);

create or replace function new_TC_pairs_Local()
returns table (source integer, target integer) AS
$$
	(select TC_temp.source, E_temp.target
	from TC_temp, E_temp
	where TC_temp.target = E_temp.source)
	except
	(select source, target
	from TC_temp);
$$ LANGUAGE SQL;

create or replace function Transitive_Closure_Local()
returns void as $$
begin
	drop table if exists TC_temp;
	create table TC_temp(source integer, target integer);
	insert into TC_temp select * from E_temp;
	while exists(select * from new_TC_pairs_Local())
	loop
		insert into TC_temp select * from new_TC_pairs_Local();
	end loop;
end;
$$ language plpgsql;


--------------
--function for checking the articulation points
drop function if exists articulation_points(int);
CREATE OR REPLACE FUNCTION articulation_points()
  RETURNS TABLE (
    node int) AS 
$func$
declare
	rec record;
	tc_temp_count int;
	tc_count int;
BEGIN
    drop table if exists articulation_points;
    CREATE TABLE articulation_points (node integer);
    drop table if exists distinct_nodes;
    create table distinct_nodes(node integer);

    insert into distinct_nodes(select distinct c.node from (select distinct e.source node from E e union select distinct e_1.target node from E e_1) c);
    drop table if exists tc_computation;
    create table tc_computation(node integer);

  
     FOR rec IN	
	      SELECT c.node from distinct_nodes c
   LOOP
	 delete from E_temp;
	 insert into E_temp(select * from (select * from E where target <>rec.node)a where a.source<>rec.node);
	 perform  Transitive_Closure_Local();
	 select count(*) into tc_temp_count from tc_temp;
	 select count(*) into tc_count from (select * from TC where target <>rec.node)a where a.source<>rec.node;
	 if tc_temp_count <> tc_count then
		insert into tc_computation values(rec.node);
	end if;
   END LOOP;
   
   
    RETURN QUERY
    SELECT c.node from tc_computation c;
  
END
$func$ LANGUAGE plpgsql; 
select node from articulation_points();
