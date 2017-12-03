---3---
--power set
drop table if exists A;
create table A(x integer);
insert into  A values(1);
insert into  A values(2);
insert into  A values(3);
insert into  A values(4);
insert into  A values(5);

drop function if exists PowersetA(subset integer[]);
create or replace function powerseta(subset integer[])returns table(set_ int[]) as
$$
declare num_rec integer;
counter integer;
b record;
c record;
begin
	drop table if exists powerset;
	create table powerset(element int[]);
	insert into powerset values('{}');
	
	select count(*) into num_rec from A;

	for b in select distinct x from A loop
		drop table if exists powerset_temp;
		create table powerset_temp(element int[]);
		for c in select distinct element from powerset
		loop
			insert into powerset_temp(
			select array(
			(select unnest(c.element) from powerset
			union
			select  b.x)) order by 1); 
		end loop;
		insert into powerset(select * from powerset_temp);
	end loop;
	return query (select element from powerset);
end;
$$ language plpgsql;

select powerseta(array(select x from A));


