# report_aaircraft.py
# CSC 370 - Summer 2020 
#
# Name: Adam Kwan
# Studeent Number: V00887099

import psycopg2, sys



def print_entry(aircraft_id, airline, model_name, num_flights, flight_hours, avg_seats_full, seating_capacity):
    print("%-5s (%s): %s"%(aircraft_id, model_name, airline))
    print("    Number of flights : %d"%num_flights)
    print("    Total flight hours: %d"%flight_hours)
    print("    Average passengers: (%.2f/%d)"%(avg_seats_full,seating_capacity))
    
psql_user = 'adamkwan' #Change this to your username
psql_db = 'adamkwan' #Change this to your personal DB name
psql_password = 'easypassword1' #Put your password (as a string) here
psql_server = 'studdb1.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)
cursor = conn.cursor()
cursor.execute("""
with flight_count as
(select aircraft_id,count(flight_id) as num_flights from aircraft natural left outer join flights group by aircraft_id),
durations as 
(select flight_id,aircraft_id,
(case 
when flight_id is null then 0
else (extract(epoch from arrival_time) - extract(epoch from departure_time))/3600
end) as duration from aircraft natural left outer join flights),
total_durations as 
(select aircraft_id, round(sum(duration)) as flight_hours from durations group by aircraft_id),
passenger_count as 
(select flight_id,count(passenger_id) as seats_full from reservations group by flight_id),
count_per_flight as 
(select * from flights natural left outer join passenger_count),
count_per_plane as 
(select aircraft_id,
(case 
when seats_full is null then 0
else seats_full
end) as seats_full from aircraft natural left outer join count_per_flight),
avg_seats as 
(select aircraft_id, avg(seats_full) as avg_seats_full from aircraft natural join count_per_plane group by aircraft_id)
select distinct aircraft_id,airline,model,num_flights,flight_hours,avg_seats_full,capacity
from aircraft natural join flight_count natural join total_durations natural join avg_seats
order by aircraft_id;
""")

while True:
    row = cursor.fetchone()
    if row is None:
        break
    print_entry(row[0],row[1],row[2],row[3],row[4],row[5],row[6])


cursor.close()
conn.close()