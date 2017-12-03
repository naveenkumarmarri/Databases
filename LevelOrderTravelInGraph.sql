---2---------
--Write a Postgres program that computes the pairs (id1, id2) such that id1
--and id2 belong to the same generation in the Parent-Child relation and
--id1 6= id2. (id1 and id2 belong to the same generation if their distance to
--the root in the Parent-Child relation is the same.)

drop table if exists parent_child;
create table parent_child(pid integer, sid integer);
insert into parent_child values(1,2);
insert into parent_child values(1,3);
insert into parent_child values(2,4);

insert into parent_child values(3,6);
insert into parent_child values(6,8);
insert into parent_child values(1,5);
insert into parent_child values(5,9);



drop table if exists child_same_level;
create table child_same_level (id1 integer,id2 integer);
create or replace function level_order_traversal() returns void as
$$
declare rec_count integer;
begin
	
	insert into child_same_level(select distinct pid,pid from parent_child where pid not in (select sid from parent_child));
	while exists (select * from parent_child)
	loop
		insert into child_same_level(select distinct a.sid,b.sid from
			(select * from parent_child where pid not in (select sid from parent_child))a,
			(select * from parent_child where pid not in (select sid from parent_child))b
			where a.sid<>b.sid);
		select count(*) into rec_count from parent_child;
		if rec_count = 1 then
			insert into child_same_level(select sid,sid from parent_child);
		end if;
		delete from parent_child where pid not in (select sid from parent_child);
		
	end loop;
end;
$$ language plpgsql;

select level_order_traversal();
select * from child_same_level order by 1,2;
