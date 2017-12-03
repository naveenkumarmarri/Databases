drop table if exists graph;
create table graph(source integer,target integer,weight integer);

insert into graph values(1,2,5);
insert into graph values(2,1,5);
insert into graph values(1,3,3);
insert into graph values(3,1,3);
insert into graph values(2,3,2);
insert into graph values(3,2,2);
insert into graph values(2,5,2);
insert into graph values(5,2,2);
insert into graph values(3,5,4);
insert into graph values(5,3,4);
insert into graph values(2,4,8);
insert into graph values(4,2,8);

--code for minimum spanning tree
drop function if exists minimum_spanning_tree();
create or replace function minimum_spanning_tree() returns table(source_node integer,target_node integer) as
$$
declare num_nodes integer;
	rand_node integer;
	min_target_weight integer;
	counter integer;
	found_source integer;
	found_target integer;
begin
	counter=0;
	drop table if exists visited_nodes;
	create table visited_nodes(node integer);
	drop table if exists spanning_tree;
	create table spanning_tree(source integer,target integer,weight integer);
	
	select count(distinct a.node) into num_nodes from (select source node from graph union select target  node from graph)a;
	--first step, randomly selecting node from the graph
	insert into visited_nodes(select source  from graph where source not in (select node from visited_nodes )order by random() limit 1);
	while(counter<num_nodes) loop
		select min(weight) into min_target_weight from graph where source in (select node from visited_nodes)and target not in(select node from visited_nodes);
		select source,target into found_source,found_target from graph where source in (select node from visited_nodes) 
				and target not in (select node from visited_nodes) and weight = min_target_weight order by random() limit 1;
		insert into spanning_tree values(found_source,found_target,min_target_weight);
		insert into visited_nodes values(found_source);
		insert into visited_nodes values(found_target);
		select count(distinct node) into counter from visited_nodes;
	end loop;
	return query (select source as source_node,target as target_node from spanning_tree union select target as source_node,source as target_node from spanning_tree);
end;
$$ language plpgsql;

select source_node,target_node from minimum_spanning_tree();