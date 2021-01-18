# report_itinerary.py
# CSC 370 - Summer 2020
#
# Name: Adam Kwan
# Studeent Number: V00887099
import psycopg2, sys



def print_header(passenger_id, passenger_name):
    print("Itinerary for %s (%s)"%(str(passenger_id), str(passenger_name)) )
    
def print_entry(flight_id, airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, aircraft_model):
    print("Flight %-4s (%s):"%(flight_id, airline))
    print("    [%s] - [%s] (%s minutes)"%(departure_time, arrival_time,duration_minutes))
    print("    %s -> %s (%s: %s)"%(source_airport_name, dest_airport_name, aircraft_id, aircraft_model))



if len(sys.argv) < 2:
    print('Usage: %s <passenger id>'%sys.argv[0], file=sys.stderr)
    sys.exit(1)

psql_user = 'adamkwan' #Change this to your username
psql_db = 'adamkwan' #Change this to your personal DB name
psql_password = 'easypassword1' #Put your password (as a string) here
psql_server = 'studdb1.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)
cursor = conn.cursor()

passenger_id = sys.argv[1]

cursor.execute("select passenger_name from passengers where passenger_id = %s;", (passenger_id,))
row = cursor.fetchone()
if row is None:
    print("Error: No passenger found with id: " + passenger_id)
    sys.exit(1)
print_header(passenger_id, row[0])

cursor.execute("""
with source_airports as 
(select flight_id, airport_name as source_airport_name from flights inner join airports on flights.source_airport = airports.iata),
dest_airports as 
(select flight_id, airport_name as dest_airport_name from flights inner join airports on flights.destination_airport = airports.iata),
passenger_flights as 
(select flight_id from reservations where passenger_id = %s)
select distinct flight_id,airline,source_airport_name,dest_airport_name,departure_time,arrival_time,(extract(epoch from arrival_time)/60 - extract(epoch from departure_time)/60)::numeric::integer as duration,aircraft_id,model
from flights natural join passenger_flights natural join source_airports natural join dest_airports natural join aircraft
order by departure_time;
""", (passenger_id,))

row = cursor.fetchone()
if row is None:
    sys.exit(1)

while True:
    if row is None:
        break
    print_entry(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8])
    row = cursor.fetchone()

cursor.close()
conn.close()