drop table if exists data;
create table data(index integer,value integer);
insert into data values (1,3);
insert into data values (2,1);
insert into data values (3,2);
insert into data values (4,0);
insert into data values (5,7);

insert into data values (6,8);
insert into data values (7,9);
insert into data values (8,11);
insert into data values (9,1);
insert into data values (10,3);

select * from data;

drop function if exists binary_heapify();
create or replace function binary_heapify() returns void as
$$
declare num_records integer;
	counter integer;
	left_value integer;
	left_index integer;
	right_value integer;
	right_index integer;
	parent_value integer;
	parent_index integer;
	smallest_index integer;
	smallest_value integer;
	cur_small integer;
	variable integer;
begin
	drop table if exists temp_data;
	create table temp_data as (select * from data);
	select count(*) into num_records from temp_data;
	for variable in 1..num_records loop
		for counter in 1..num_records/2
		loop
			
			smallest_value = 99999999;
			select index,value into parent_index,parent_value from temp_data where index = counter;
			cur_small=parent_index;
			select case when exists (select true from data where index=2*counter)='t' 
					then (select index from data where index=2*counter)  	
				else 99999999 end ,
				case when exists (select true from data where index=2*counter)='t' 
					then (select value from data where index=2*counter)  	
				else 99999999 end into left_index,left_value;
				
			select case when exists (select true from data where index=(2*counter)+1)='t'
					then (select index from data where index=(2*counter)+1) 
				else 99999999  end,
				case when exists (select true from data where index=(2*counter)+1)='t'
					then (select value from data where index=(2*counter)+1) 
				else 99999999 end into right_index,right_value;
				
			if left_value<parent_value then
				smallest_value = left_value;
				smallest_index = left_index;
				cur_small = left_index;
			else
				smallest_value = parent_value;
				smallest_index = parent_index;
			end if;
			if right_value<smallest_value then
				smallest_value = right_value;
				smallest_index = right_index;
				cur_small=right_index;
			end if;
			if cur_small <> counter  then
				update temp_data set value = smallest_value where index = parent_index;
				update temp_data set value = parent_value where index = cur_small;
			end if;
			counter = cur_small;
		end loop;
		delete from data;
	insert into data(select * from temp_data);
	end loop;
	
end;
$$ language plpgsql;

select binary_heapify();
drop function if exists insert(int);
create or replace function insert(value int) returns void as 
$$
declare max_index integer;
begin
	select max("index") into max_index from data;
	insert into data values(max_index+1, value);
	perform  binary_heapify();
end;
$$ language plpgsql;

drop function if exists "extract"();
create or replace function "extract"() returns integer as 
$$
declare min_index integer;
	last_leaf_node integer;
	last_leaf_node_value integer;
	extracted_min_value integer;
begin
	perform binary_heapify();
	select min(index) into min_index from data;
	select max(index) into last_leaf_node from data;
	select value into extracted_min_value from data where index = min_index;
	select value into last_leaf_node_value from data where index = last_leaf_node;
	update data set value=last_leaf_node_value where index = min_index;
	delete from data where index = last_leaf_node;
	perform binary_heapify();
	return extracted_min_value;
end;
$$ language plpgsql;

-----------------------
---(b) heap sort algorithm--
---here we iteratively extract the elements from the data---
--the output data is stored in sorteddata table
drop function if exists heapsort();
create or replace function heapsort() returns
void as
$$
declare counter integer;
	loop_counter integer;
begin
	select count(*) into counter from data ;
	drop table if exists sorteddata;
	create table sorteddata(index integer,value integer);
	for loop_counter in 1..counter loop
		insert into sorteddata(index,value) (select loop_counter, "extract"());
	end loop;
end;
$$ language plpgsql;

select heapsort();
select * from sortedData;

