# manage_reservations.py
# CSC 370 - Summer 2020 - Starter code for Assignment 6
#
#
# B. Bird - 06/28/2020

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
        if len(row) != 4:
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            conn.rollback()
            break
        action,flight_id,passenger_id,passenger_name = row
        
        if action.upper() not in ('CREATE','DELETE'):
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            conn.rollback()
            break
        
		#Do something with the data here
        if action.upper() == 'CREATE':
            cursor.execute("select passenger_name from passengers where passenger_name = %s;", (passenger_name,))
            if cursor.fetchone() is None:
                try:
                    cursor.execute("insert into passengers values( %s, %s );", (passenger_id,passenger_name))
                except psycopg2.IntegrityError as e:
                    print('Insertion Error on table passengers with the following info:\n')
                    print(e)
                    conn.rollback()
                    break
            try:
                cursor.execute("insert into reservations values( %s, %s );", (passenger_id,flight_id))
            except psycopg2.IntegrityError as e:
                print('Insertion Error on table reservations with the following info:\n')
                print(e)
                conn.rollback()
                break
        else: 
            try:
                cursor.execute("delete from reservations where flight_id =  %s and passenger_id = %s ;", (flight_id,passenger_id))
            except psycopg2.IntegrityError as e:
                    print('Deletion Error on table flights with the following info:\n')
                    print(e)
                    conn.rollback()
                    break
            rows_after = cursor.rowcount
            if(rows_after == 0):
                print("Error: No rows were deleted after delete statement")
                conn.rollback()
                break
		#Make sure to catch any exceptions that occur and roll back the transaction if a database error occurs.
conn.commit()
cursor.close()
conn.close()		