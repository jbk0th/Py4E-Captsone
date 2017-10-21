import requests
import sqlite3
import re

conn = sqlite3.connect("Heart_data.sqlite")
cur = conn.cursor()

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

# Possbily change zip to an intger field, see how that will work when try to
# make a graphic with it
cur.execute('''CREATE TABLE IF NOT EXISTS Zip_code (
            id INTEGER PRIMARY KEY, zip_ INTEGER UNIQUE )''')

cur.execute('''CREATE TABLE IF NOT EXISTS Links (
            hosp_id INTEGER PRIMARY KEY, state_id INTEGER, pay_id INTEGER, val_id INTEGER)''')


#base url for the json data on heart attack payment
baseurl = 'https://data.medicare.gov/api/views/c7us-v4mf/rows.json?accessType=DOWNLOAD'

get_url = requests.get(baseurl)

url_json = get_url.json()

counter = 0

# the data being input into the DB are appended with _dat, to avoid confusion with
# the similarly named databased fields
for data in url_json['data']:
    if 'AMI' not in data[17]: continue
    hosp_name_dat = data[9]
    city_dat = data[11]
    state_dat = data[12]
    # Possibly change this to an integer will see if allows graphic
    zip_dat = int(data[13])
    county_dat = data[14]
    pay_cat_dat = data[18]
    value_care_dat = data[26]
    denom_dat = data[19]
    if len(re.sub('\D','',data[20])) <= 1:
        payment_dat = None
    else: payment_dat = int(re.sub('\D','',data[20]))
            # need to convert this to an int currently has $
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
    print(counter)

    cur.execute('''INSERT OR IGNORE INTO  Hospital_data
                (hospital_name, city, county, denominator, payment, lower_est, higher_est, lat, lon) VALUES (?,?,?,?,?,?,?,?,?)''',
                (hosp_name_dat, city_dat, county_dat, denom_dat, payment_dat, lower_est_dat, higher_est_dat, lat_dat, lon_dat))
    cur.execute('INSERT OR IGNORE INTO State (state_code) VALUES (?)', (state_dat,))
    cur.execute('INSERT OR IGNORE INTO Payment_category (category) VALUES (?)', (pay_cat_dat,))
    cur.execute('INSERT OR IGNORE INTO Value_of_care (value_category) VALUES (?)', (value_care_dat,))
    cur.execute('INSERT OR IGNORE INTO Zip_code (zip_) VALUES (?)', (zip_dat,))

    # commit cached data to disk every 50 row reads
    if counter % 50 == 0:
        print('{} records committed thus far! Now on state {}'.format(counter, state_dat))
        conn.commit()

print('Total records logged: {}'.format(counter))

cur.close()
