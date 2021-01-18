-- Name: Adam Kwan
-- Studeent Number: V00887099
drop table if exists reservations;
drop table if exists flights;
drop table if exists aircraft;
drop table if exists airports;
drop table if exists passengers;
drop function if exists international_flight_trigger();
drop function if exists airline_consistency_trigger();
drop function if exists aircraft_size_trigger();
drop function if exists reservation_trigger();
drop function if exists flight_insert_update_trigger();
drop function if exists flight_delete_trigger();

create table airports(
    iata varchar(3),
    airport_name varchar(255),
    country varchar(255),
    international_status boolean,
    primary key(iata),
    constraint country_not_empty check(length(country) > 0 ),
    constraint airport_name_not_empty check(length(airport_name) > 0),
    constraint iata_formatting_constraint check(upper(iata) = iata and length(iata) = 3)
);

create table aircraft(
    aircraft_id varchar(64),
    airline varchar(255),
    model varchar(255),
    capacity int,
    primary key(aircraft_id),
    check(capacity >= 0),
    check(length(aircraft_id) > 0),
    check(length(model) > 0),
    check(length(airline) > 0)
);

create table passengers(
    passenger_id int,
    passenger_name varchar(1000),
    primary key(passenger_id),
    check(length(passenger_name) > 0)
);

create table flights(
    flight_id int,
    airline varchar(255),
    source_airport varchar(3),
    destination_airport varchar(3),
    departure_time timestamp,
    arrival_time timestamp,
    aircraft_id varchar(64),
    primary key(flight_id),
    foreign key(source_airport) references airports(iata),
    foreign key(destination_airport) references airports(iata),
    foreign key(aircraft_id) references aircraft(aircraft_id),
    check(source_airport <> destination_airport),
    check(length(airline) > 0),
    check(departure_time < arrival_time)
);

create table reservations(
    passenger_id int,
    flight_id int,
    primary key(passenger_id, flight_id),
    foreign key(passenger_id) references passengers(passenger_id),
    foreign key(flight_id) references flights(flight_id)
);


create function international_flight_trigger()
returns trigger as 
$BODY$
begin 
if (select country from airports where iata = new.source_airport) 
    <> (select country from airports where iata = new.destination_airport)
then 
    if (select international_status from airports where iata = new.source_airport) = false 
        OR (select international_status from airports where iata = new.destination_airport) = false
    then 
        raise exception 'International flight with a non international airport';
    end if;
end if;
return new;
end 
$BODY$
language plpgsql;

create function airline_consistency_trigger()
returns trigger as 
$BODY$
begin 
if (select airline from aircraft where aircraft_id = new.aircraft_id) <> new.airline
then 
    raise exception 'Flight airline does not match aircraft airline';
end if;
return new;
end
$BODY$
language plpgsql;

create function aircraft_size_trigger()
returns trigger as 
$BODY$
begin 
if (select count(passenger_id) from (
    select passenger_id, flight_id from reservations where flight_id = new.flight_id) as t1
    group by flight_id) > (select capacity from aircraft where aircraft_id = new.aircraft_id)
then 
    raise exception 'New aircraft does not have capacity for the exisiting reservations';
end if;
return new;
end
$BODY$
language plpgsql;

create function reservation_trigger()
returns trigger as 
$BODY$
begin 
if ((select capacity from aircraft 
    where aircraft_id = (select aircraft_id from flights where flight_id = new.flight_id))
    <= (select count(passenger_id) from (
    select passenger_id, flight_id from reservations where flight_id = new.flight_id) as t1
    group by flight_id))
then 
    raise exception 'Flight is full';
end if;
return new;
end
$BODY$
language plpgsql;

create function flight_insert_update_trigger()
returns trigger as 
$BODY$
begin
if (
    with filtered_flights as 
    (select * from flights where aircraft_id = new.aircraft_id),
    before_and_after as 
    (select *, 
    lag(destination_airport) over(order by departure_time) as prev_dest,
    lead(source_airport) over(order by departure_time) as next_source
    from filtered_flights)
    select 
    sum(case 
    when prev_dest is null or next_source is null then 0 
    when prev_dest = source_airport and next_source = destination_airport then 0
    else 1
    end) from before_and_after) > 0 
then
    raise exception 'Source and/or destination airports do not match this aircraft locations';
end if;
if (
    with filtered_flights as 
    (select * from flights where aircraft_id = new.aircraft_id),
    before_and_after as 
    (select *, 
    lag(arrival_time) over(order by departure_time) as prev_time,
    lead(departure_time) over(order by departure_time) as next_time
    from filtered_flights)
    select 
    sum(case 
    when prev_time is null or next_time is null then 0 
    when (extract(epoch from departure_time) - extract(epoch from prev_time))/60 > 60 and (extract(epoch from next_time) - extract(epoch from arrival_time))/60 > 60 then 0
    else 1
    end) from before_and_after) > 0 
then
    raise exception 'This aircraft does not have 60 minutes between flights';
end if;
return new;
end
$BODY$
language plpgsql;

create function flight_delete_trigger()
returns trigger as 
$BODY$
declare 
target_aircraft_id varchar(64);
begin
target_aircraft_id := (select aircraft_id from flights where flight_id = old.flight_id);
if (
    with filtered_flights as 
    (select * from flights where aircraft_id = target_aircraft_id),
    before_and_after as 
    (select *, 
    lag(destination_airport) over(order by departure_time) as prev_dest,
    lead(source_airport) over(order by departure_time) as next_source
    from filtered_flights),
    target_flight as 
    (select * from before_and_after where flight_id = old.flight_id)
    select 
    sum(case 
    when prev_dest is null or next_source is null then 0 
    else 1
    end) from target_flight) > 0 
then
    raise exception 'Deleting this flight makes future flights invalid';
end if;
return old;
end
$BODY$
language plpgsql;



create trigger international_flight_constraint
    before insert or update on flights
    for each row 
    execute procedure international_flight_trigger();

create trigger airline_consistency_constraint
    before insert or update on flights
    for each row 
    execute procedure airline_consistency_trigger();

create trigger aircraft_size_constraint
    before update on flights
    for each row 
    execute procedure aircraft_size_trigger();

create trigger reservation_constraint
    before insert on reservations
    for each row 
    execute procedure reservation_trigger();

create trigger flight_insert_update_constraints 
    after insert or update on flights 
    for each row 
    execute procedure flight_insert_update_trigger();

create trigger flight_delete_constraints 
    before delete on flights 
    for each row 
    execute procedure flight_delete_trigger();