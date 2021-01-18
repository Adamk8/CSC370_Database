# report_all_flights.py
# CSC 370 - Summer 2020
#
# Name: Adam Kwan
# Studeent Number: V00887099

import psycopg2, sys

def print_entry(flight_id, airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, aircraft_model, seating_capacity, seats_full):
    print("Flight %s (%s):"%(flight_id,airline))
    print("    [%s] - [%s] (%s minutes)"%(departure_time,arrival_time,duration_minutes))
    print("    %s -> %s"%(source_airport_name,dest_airport_name))
    print("    %s (%s): %s/%s seats booked"%(aircraft_id, aircraft_model,seats_full,seating_capacity))
    
psql_user = 'adamkwan' #Change this to your username
psql_db = 'adamkwan' #Change this to your personal DB name
psql_password = 'easypassword1' #Put your password (as a string) here
psql_server = 'studdb1.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)
cursor = conn.cursor()

cursor.execute("""
with passenger_count as 
(select flight_id,count(passenger_id) as seats_full from reservations group by flight_id),
flight_passenger_count as 
(select flight_id,
(case 
when seats_full is null then 0
else seats_full
end) as seats_full
from flights natural left outer join passenger_count),
source_airports as 
(select flight_id, airport_name as source_airport_name from flights inner join airports on flights.source_airport = airports.iata),
dest_airports as 
(select flight_id, airport_name as dest_airport_name from flights inner join airports on flights.destination_airport = airports.iata)
select distinct flight_id,airline,source_airport_name,dest_airport_name,departure_time,arrival_time,(extract(epoch from arrival_time)/60 - extract(epoch from departure_time)/60)::numeric::integer as duration,aircraft_id,model,capacity,seats_full
from flights natural join airports natural join aircraft natural join flight_passenger_count natural join source_airports natural join dest_airports
order by departure_time;
""")

while True:
    row = cursor.fetchone()
    if row is None:
        break
    print_entry(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10])


cursor.close()
conn.close()