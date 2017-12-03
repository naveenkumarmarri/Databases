---problem 4--
--kmeans algorithm--
drop table if exists data;
create table data(x float, y float);
insert into data values(1,1);
insert into data values(1.5,2);
insert into data values(3,4);
insert into data values(5,7);
insert into data values(3.5,5);
insert into data values(4.5,5);
insert into data values(3.5,4.5);


--helper function for finding distance between points--
drop function if exists distance_two_points(float,float,float,float);
create or replace function distance_two_points(x1 float, y1 float, x2 float, y2 float) returns float as
$$
declare distance float;
begin
	select sqrt(power((x2-x1),2)+power((y2-y1),2)) into distance;
	return distance;
end;
$$ language plpgsql;

--kmeans algorithm--
drop function if exists kmeans(int,float);
create or replace function kmeans(k int,threshold float) returns table(clust_id integer,point_x float,point_y float) as 
$$
declare num_points integer;
distance float;
min_distance float;
temp_cluster_id integer;
b record;
error float;
centroid record;
x_mean float;
y_mean float;
begin
	select count(*) into num_points from data;
	--exception handling--
	if k>num_points then
		raise exception 'number of clusters more than input points';
	else
		drop table if exists centroids;
		create table centroids(id  serial ,x float, y float);
		insert into centroids(x,y)(select * from data order by random() limit k);

		drop table if exists cluster_assign;
		create table cluster_assign(cluster_id integer,x float, y float);
		error = 1000;
		--checking for threhold convergence--
		while(error>threshold) loop
			error = 0;
			delete from cluster_assign;
			for b in (select * from data)
			loop
				min_distance = 10000000;
				temp_cluster_id = 0;
				for centroid in (select * from centroids) loop
					select distance_two_points(b.x,b.y,centroid.x,centroid.y) into distance;
					if distance<min_distance then
						min_distance=distance;
						temp_cluster_id = centroid.id;
					end if;
				end loop;
				insert into cluster_assign values(temp_cluster_id,b.x,b.y);
			end loop;
			--used l2 norm for finding convergence
			for centroid in (select * from centroids) loop
				select avg(x),avg(y) into x_mean,y_mean from cluster_assign where cluster_id = centroid.id;
				error = error + distance_two_points(x_mean,y_mean,centroid.x,centroid.y);
				update centroids set x=x_mean,y=y_mean where id =centroid.id;
			end loop;
		end loop;
		return query (select * from cluster_assign);
	end if;
end;
$$language plpgsql;

--running the algorithm
select clust_id,point_x,point_y from kmeans(3,0.001);

select * from cluster_assign;
select * from centroids;