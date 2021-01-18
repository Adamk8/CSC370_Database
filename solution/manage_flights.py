# manage_flights.py
# CSC 370 - Summer 2020
#
# Name: Adam Kwan
# Studeent Number: V00887099

import sys, csv, psycopg2

if len(sys.argv) < 2:
    print("Usage: %s <input file>"%sys.argv[0],file=sys.stderr)
    sys.exit(1)
    
input_filename = sys.argv[1]

psql_user = 'adamkwan' #Change this to your username
psql_db = 'adamkwan' #Change this to your personal DB name
psql_password = 'easypassword1' #Put your password (as a string) here
psql_server = 'studdb1.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)
cursor = conn.cursor()

with open(input_filename) as f:
    for row in csv.reader(f):
        if len(row) == 0:
            continue #Ignore blank rows
        action = row[0]
        if action.upper() == 'DELETE':
            if len(row) != 2:
                print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
                
            flight_id = row[1]
            try:
                cursor.execute("delete from flights where flight_id =  %s  ;", (flight_id,))
            except psycopg2.IntegrityError as e:
                print('Deletion Error on table flights with the following info:\n')
                print(e)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
                
            except psycopg2.InternalError as e:
                print('Internal Error deleting from table flights with the following info:\n')
                print(e)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
                
            except psycopg2.DataError as e:
                print('Data Error deleting table flights with the following info:\n')
                print(e)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
            
            rows_after = cursor.rowcount
            if(rows_after == 0):
                print("Error: No rows were deleted after delete statement")
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
            


        elif action.upper() in ('CREATE','UPDATE'):
            if len(row) != 8:
                print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
                
            flight_id = row[1]
            airline = row[2]
            src,dest = row[3],row[4]
            departure, arrival = row[5],row[6]
            aircraft_id = row[7]
            if action.upper() == 'CREATE':
                try:
                    cursor.execute("insert into flights values( %s, %s, %s, %s, %s, %s, %s);",(flight_id,airline,src,dest,departure,arrival,aircraft_id))
                except psycopg2.IntegrityError as e:
                    print('Insertion Error on table flights with the following info:\n')
                    print(e)
                    conn.rollback()
                    cursor.close()
                    conn.close()
                    sys.exit(1)
                    
                except psycopg2.InternalError as e:
                    print('Internal Error inserting into table flights with the following info:\n')
                    print(e)
                    conn.rollback()
                    cursor.close()
                    conn.close()
                    sys.exit(1)
                except psycopg2.DataError as e:
                    print('Data Error inserting into table flights with the following info:\n')
                    print(e)
                    conn.rollback()
                    cursor.close()
                    conn.close()
                    sys.exit(1)
                    
            elif action.upper() == 'UPDATE':
                try:
                    cursor.execute('''update flights set 
                                    airline = %s,
                                    source_airport = %s,
                                    destination_airport = %s,
                                    departure_time = %s,
                                    arrival_time = %s,
                                    aircraft_id = %s
                                    where flight_id = %s;''', (airline,src,dest,departure,arrival,aircraft_id,flight_id))
                except psycopg2.InternalError as e:
                    print('Internal Error updating table flights with the following info:\n')
                    print(e)
                    conn.rollback()
                    cursor.close()
                    conn.close()
                    sys.exit(1)
                    

        else:
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            conn.rollback()
            cursor.close()
            conn.close()
            sys.exit(1)
            


conn.commit()
cursor.close()
conn.close()