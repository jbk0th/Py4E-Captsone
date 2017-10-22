import requests
import sqlite3
import re

conn = sqlite3.connect("Heart_data.sqlite")
cur = conn.cursor()

# clear all tables before starting data acquistion

cur.executescript('''
DROP TABLE IF EXISTS Hospital_data;
DROP TABLE IF EXISTS State;
DROP TABLE IF EXISTS Payment_category;
DROP TABLE IF EXISTS Value_of_care;
DROP TABLE IF EXISTS Zip_code;
DROP TABLE IF EXISTS Links;
''')

''' Note: for the amount of records that will actually be downloaded and stored in this TABLE
this many tables is overkill, all the ~2300 records could be put in the same table and work fine
for the graphic id like to create. However, want experience setting up something more complex
for experience'''

#creation of all tables to be used in the database, separate tables were made
#for any values that would be duplicated many time in a tables

# The Links table serves as the linking table for all the records to JOIN on keys
# and return the complete information for each row

cur.execute('''CREATE TABLE IF NOT EXISTS Hospital_data (
            id INTEGER PRIMARY KEY, hospital_name TEXT,
            city TEXT, county TEXT, denominator TEXT, payment INTEGER,
            lower_est INTEGER, higher_est INTEGER, lat INTEGER, lon INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS State (
            id INTEGER PRIMARY KEY , state_code TEXT UNIQUE)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Payment_category (
            id INTEGER PRIMARY KEY, category TEXT UNIQUE)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Value_of_care (
            id INTEGER PRIMARY KEY, value_category TEXT UNIQUE)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Zip_code (
            id INTEGER PRIMARY KEY, zip_ INTEGER UNIQUE )''')

cur.execute('''CREATE TABLE IF NOT EXISTS Links (
            hosp_id INTEGER PRIMARY KEY, state_id INTEGER, pay_id INTEGER, val_id INTEGER, zip_id INTEGER)''')


#base url for the json data on heart attack payment
baseurl = 'https://data.medicare.gov/api/views/c7us-v4mf/rows.json?accessType=DOWNLOAD'

get_url = requests.get(baseurl)

url_json = get_url.json()

counter = 0

# the data being input into the DB are appended with _dat, to avoid confusion with
# the similarly named database fields
for data in url_json['data']:
    if 'AMI' not in data[17]: continue
    hosp_name_dat = data[9]
    city_dat = data[11]
    state_dat = data[12]
    zip_dat = int(data[13])
    county_dat = data[14]
    pay_cat_dat = data[18]
    value_care_dat = data[26]
    denom_dat = data[19]
    # The below statments remove any non decimal digit character, to return data ammenable to conversion to an integer type
    if len(re.sub('\D','',data[20])) <= 1:
        payment_dat = None
    else: payment_dat = int(re.sub('\D','',data[20]))
    if len(re.sub('\D','',data[21])) <= 1:
        lower_est_dat = None
    else: lower_est_dat = int(re.sub('\D','',data[21]))
    if len(re.sub('\D','',data[22])) <= 1:
        higher_est_dat = None   # need to convert this to an int currently has $
    else: higher_est_dat = int(re.sub('\D','',data[22]))
    lat_dat = data[30][1]
    lon_dat = data[30][2]
    #iteration counter to enable db commit to disk every 50 rows
    counter += 1

    # Inserting all data in the appropriate table
    cur.execute('''INSERT OR IGNORE INTO  Hospital_data
                (hospital_name, city, county, denominator, payment, lower_est, higher_est, lat, lon) VALUES (?,?,?,?,?,?,?,?,?)''',
                (hosp_name_dat, city_dat, county_dat, denom_dat, payment_dat, lower_est_dat, higher_est_dat, lat_dat, lon_dat))
    cur.execute('INSERT OR IGNORE INTO State (state_code) VALUES (?)', (state_dat,))
    cur.execute('INSERT OR IGNORE INTO Payment_category (category) VALUES (?)', (pay_cat_dat,))
    cur.execute('INSERT OR IGNORE INTO Value_of_care (value_category) VALUES (?)', (value_care_dat,))
    cur.execute('INSERT OR IGNORE INTO Zip_code (zip_) VALUES (?)', (zip_dat,))

    '''Below accessing all the input record ids to construst links table
    to acess complete row info for a hosptal_Data entry'''

    cur.execute('SELECT id FROM Hospital_data WHERE hospital_name=? AND city=?',(hosp_name_dat, city_dat)) #understand the limit 1 is superfluous if the entry is truly unique
    hosp_id_dat = cur.fetchone()[0]

    cur.execute('SELECT id FROM State WHERE state_code=?',(state_dat,))
    state_id_dat = cur.fetchone()[0]

    cur.execute('SELECT id FROM Payment_category WHERE category=?',(pay_cat_dat,))
    pay_id_dat = cur.fetchone()[0]

    cur.execute('SELECT id FROM Value_of_care WHERE value_category=?',(value_care_dat,))
    val_id_dat = cur.fetchone()[0]

    cur.execute('SELECT id FROM Zip_code WHERE zip_=?',(zip_dat,))
    zip_id_dat = cur.fetchone()[0]
    #insert all ids into linker table to join on
    cur.execute('''INSERT OR IGNORE INTO Links
    (hosp_id, state_id, pay_id, val_id, zip_id) VALUES (?, ?, ?, ?, ?)''',
    (hosp_id_dat, state_id_dat, pay_id_dat, val_id_dat, zip_id_dat))

    # commit the data to disk every 50 iterations
    if counter % 50 == 0:
        conn.commit()
        print('{} records committed thus far! Now on state {}'.format(counter, state_dat))

#Final commit to catch all records not logged before close the DB connection
conn.commit()
print('{} records committed thus far! Now on state {}'.format(counter, state_dat))

print('Total records logged: {}'.format(counter))

cur.close()


''' Examples of Many-to-Many Join query's for diagnostic purposes,
IMPT Note: Start outer most then keep moving in with joins till have full row one would like.

Following Examples tested in SQLite DB browser:

# Joining Links table with Hospital_data table

SELECT Hospital_data.*, Links.*
-- Payment_category.category, State.state_code, Value_of_care.value_category, Zip_code.zip_
FROM Hospital_data
LEFT JOIN Links
ON Hospital_data.id = Links.hosp_id
LIMIT 5

#result is full table with all linking ids present,

--Next is full join from all tables
SELECT Hospital_data.*, Links.*, Payment_category.category, State.state_code, Value_of_care.value_category, Zip_code.zip_
-- Payment_category.category, State.state_code, Value_of_care.value_category, Zip_code.zip_
FROM Hospital_data
LEFT JOIN Links ON Hospital_data.id = Links.hosp_id
-- first gets outermost Hospital_data into, the join the Links table so the other tables can be joined
LEFT JOIN Payment_category ON Links.pay_id = Payment_category.id
-- payment cateogry now joined, now 1 more work further in
LEFT JOIN State ON Links.state_id = State.id
--
LEFT JOIN Value_of_care ON Links.val_id = Value_of_care.id
--
LEFT JOIN Zip_code ON Links.zip_id = Zip_code.zip_
--
--- Example of a wonky record edge case WHERE Hospital_data.id=2695
LIMIT 5
'''
