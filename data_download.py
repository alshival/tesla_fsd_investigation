import pandas as pd
import requests
import time
from datetime import date, datetime

from config import *

########################################
# Main function to download complaints
########################################
# Function to get complaints from NHTSA API
import requests
from requests.exceptions import Timeout, RequestException
import time

# Function to get complaints from NHTSA API
def get_complaints(make, model, model_year, retries=3, timeout=30):
    # Construct the API URL
    url = f"https://api.nhtsa.gov/complaints/complaintsByVehicle?make={make}&model={model}&modelYear={model_year}"

    attempt = 0

    while attempt < retries:
        try:
            # Make the GET request to the NHTSA API with a timeout
            response = requests.get(url, timeout=timeout)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Return the JSON response
                return response.json().get('results', [])
            elif response.status_code == 400:
                return None
            else:
                # Return an error message
                return None
        
        except Timeout:
            # Handle timeout exception
            attempt += 1
            print(f"Attempt {attempt} timed out. Retrying...")
            time.sleep(5)  # wait before retrying
        except RequestException as e:
            # Handle other request exceptions
            print(f"Request failed: {e}")
            return None

    # If all attempts fail, return None
    print("All attempts to contact the API have failed.")
    return None


########################################
# Update Model Years
########################################
# Function to get all Model Years
# Function to get all Model Years
def get_model_years(retries=3, timeout=30):
    url = "https://api.nhtsa.gov/products/vehicle/modelYears?issueType=c"
    
    attempt = 0

    while attempt < retries:
        try:
            # Make the GET request to the NHTSA API with a timeout
            response = requests.get(url, timeout=timeout)
            
            # Check if the request was successful
            if response.status_code == 200:
                return pd.DataFrame(response.json().get('results', []))
            else:
                return f"Error: {response.status_code}"
        
        except Timeout:
            # Handle timeout exception
            attempt += 1
            print(f"Attempt {attempt} timed out. Retrying...")
            time.sleep(5)  # wait before retrying
        except RequestException as e:
            # Handle other request exceptions
            print(f"Request failed: {e}")
            return None

    # If all attempts fail, return an error message
    print("All attempts to contact the API have failed.")
    return "Error: All attempts to contact the API have failed."

# Fetch model years
model_years = get_model_years()

# Save model years to database
db = pg_connect()
model_years.to_sql('model_years',db,index=False,if_exists='replace')

########################################
# Update Makes
########################################
# Function to get all Makes for the Model Year
# Function to get makes for a specific year
def get_makes_for_year(year, retries=3, timeout=30):
    url = f"https://api.nhtsa.gov/products/vehicle/makes?modelYear={year}&issueType=c"
    
    attempt = 0

    while attempt < retries:
        try:
            # Make the GET request to the NHTSA API with a timeout
            response = requests.get(url, timeout=timeout)
            
            # Check if the request was successful
            if response.status_code == 200:
                return pd.DataFrame(response.json().get('results', []))
            else:
                return f"Error: {response.status_code}"
        
        except Timeout:
            # Handle timeout exception
            attempt += 1
            print(f"Attempt {attempt} timed out. Retrying...")
            time.sleep(5)  # wait before retrying
        except RequestException as e:
            # Handle other request exceptions
            print(f"Request failed: {e}")
            return None

    # If all attempts fail, return an error message
    print("All attempts to contact the API have failed.")
    return "Error: All attempts to contact the API have failed."

db = pg_connect()
if 'makes_for_year' in pg_tables():
    years_to_exclude = pg_query('select distinct "modelYear" from makes_for_year')['modelYear'].to_list()
else:
    years_to_exclude = []

for year in model_years[model_years['modelYear'].astype(int)>=2016]['modelYear']:
    if (year >= str(date.today().year)) | (year not in years_to_exclude):
        print(f'Downloading makes for year {year}')
        makes_for_year = get_makes_for_year(year)
        makes_for_year['updated_on'] = date.today()
        makes_for_year.to_sql('makes_for_year',db,index=False,if_exists='append')
db.dispose()
# Remove duplicate rows from the table
pg_execute(""" 
DROP TABLE IF EXISTS makes_for_year_backup
""")
pg_execute(""" 
CREATE TABLE makes_for_year_backup AS
with tbl as (
select 
    *,
    rank() over (partition by "modelYear",make order by updated_on desc) rnk
from makes_for_year
order by updated_on desc
)
select distinct on ("modelYear","make")
    "modelYear",
    "make",
    "updated_on"
from tbl where rnk = 1
""")
pg_execute(f"DELETE FROM makes_for_year;")

pg_execute(f"INSERT INTO makes_for_year SELECT * FROM makes_for_year_backup;")
pg_execute(f"DROP TABLE makes_for_year_backup;")
print(f"makes_for_year table cleaned")

########################################
# Update Models
########################################
# Function to get all Models for the Make and Model Year
def get_models_for_make_year(make, year, retries=3, timeout=30):
    url = f"https://api.nhtsa.gov/products/vehicle/models?modelYear={year}&make={make}&issueType=c"
    
    attempt = 0

    while attempt < retries:
        try:
            response = requests.get(url, timeout=timeout)
            
            if response.status_code == 200:
                return response.json().get('results', [])
            else:
                return f"Error: {response.status_code}"
        
        except Timeout:
            attempt += 1
            print(f"Attempt {attempt} timed out. Retrying...")
            time.sleep(5)
        except RequestException as e:
            print(f"Request failed: {e}")
            return None

    print("All attempts to contact the API have failed.")
    return "Error: All attempts to contact the API have failed."

if 'models_for_make_year' in pg_tables():
    makes_for_year  = pg_query(""" 
select distinct 
	"modelYear",
	make
from makes_for_year 
where "modelYear"::int >= extract(year from current_date)
union all
select distinct 
	"modelYear",
	make
from makes_for_year 
where "modelYear"::int >= extract(year from current_date) - 5
and "modelYear"::int < extract(year from current_date)
and ("modelYear",make) not in (select "modelYear",make from models_for_make_year)
        """)
else:
    makes_for_year  = pg_query(""" 
    select distinct 
        "modelYear",
        make
    from makes_for_year 
    where "modelYear"::int >= 2019
        """)

all_models = []
if len(makes_for_year) > 0:
    sample_size = min(500,len(makes_for_year))
    makes_for_year = makes_for_year.sample(sample_size)
    db = pg_connect()
    for _,row in makes_for_year.iterrows():
        print(f"Downloading {row['make']} {row['modelYear']} models")
        # Download models
        try:
            download = get_models_for_make_year(row['make'],str(row['modelYear']))
        except Exception as e:
            print("Download failed. Will try again next update.")
        # add to list
        print(download)
        payload = pd.DataFrame(download)
        if len(payload) > 0:
            payload.to_sql('models_for_make_year',db,index=False, if_exists='append')
        time.sleep(2)
    # Clean database
    pg_clean_table('models_for_make_year')
    print(f"models_for_make_year table cleaned")
########################################
# Update Complaints
########################################
def update_complaints(make_model_year):
    for _,row in make_model_year.iterrows():
        make_complaints = []
        print(f'Downloading data for {row['modelYear']} {row['make']} {row['model']}')
        complaints = get_complaints(row['make'],row['model'],row['modelYear'])
        if complaints:
            for c in complaints:
                c['make'] = row.get('make',None)
                c['model'] = row.get('model',None)
                c['modelYear'] = row.get('modelYear',None)
                c['products'] = json.dumps(c.get('products',{}))
                c['updated_on'] = date.today()
                make_complaints.append(c)
                make_df = pd.DataFrame(make_complaints)
            db = pg_connect()
            make_df.to_sql('complaints',db,index=False,if_exists='append')
            print(f'Complaint data for {row['modelYear']} {row['make']} {row['model']} updated')
            db.dispose()
        time.sleep(1)

########################################
# Update Tesla
########################################
make_model_year = pg_query("""
select distinct
    models_for_make_year.make,models_for_make_year."modelYear",models_for_make_year.model
from models_for_make_year
    where models_for_make_year."modelYear"::int >= extract(year from current_date) - 5
    and models_for_make_year.make ='TESLA'
""")

update_complaints(make_model_year)
print(f"Tesla data downloaded.")

########################################
# Update rest on random sample cadence
########################################
"""
We randomly sample make/model/modelYear data that has not been updated in the past week.
Each day this is run, 500 such combinations are updated
"""
make_model_year = pg_query(f""" 
with refresh_table as (
    SELECT DISTINCT
        make,model,"modelYear",random()
    from complaints
    where "modelYear"::int >= EXTRACT(YEAR FROM current_date) - 5
    and (CURRENT_DATE - updated_on::Date) > 5
    order by RANDOM() 
    limit 700
),
new_table as (
    select distinct
            make,model,"modelYear",random()
    from models_for_make_year
        where (make,model,"modelYear") not in (select make,model,"modelYear" from complaints)
    order by random() limit 200
)
select make,model,"modelYear" from refresh_table
union all
select make,model,"modelYear" from new_table
""")
if len(make_model_year) > 0:
    update_complaints(make_model_year)
print(make_model_year)
print(f"Stale data randomly updated.")

# for t in pg_tables():
#     pg_clean_table(t)

pg_execute(""" 
DROP TABLE IF EXISTS complaints_backup
""")

pg_execute(""" 
CREATE TABLE complaints_backup AS
with tbl as (
select 
    *,
    rank() over (partition by complaints."odiNumber" order by updated_on desc) rnk
from complaints
order by updated_on desc
)
select distinct on ("odiNumber")
    "odiNumber",
    manufacturer,
    crash,
    fire,
    "numberOfInjuries",
    "numberOfDeaths",
    "dateOfIncident",
    "dateComplaintFiled",
    "vin",
    "components",
    "summary",
    "products",
    "make",
    "model",
    "modelYear",
    "updated_on"
from tbl where rnk = 1
""")

pg_execute(f"DELETE FROM complaints;")

pg_execute(f"INSERT INTO complaints SELECT * FROM complaints_backup;")
pg_execute(f"DROP TABLE complaints_backup;")
print(f"Stale data from complaints")

##########################
# Update car_sales
##########################
car_sales = pd.read_csv('car_sales.csv')
import re
car_sales['Automaker'] = car_sales.Automaker.apply(lambda x: re.sub(',$','',x).split(','))

complaints = []
for _,row in car_sales.iterrows():
    complain = 0
    for maker in row['Automaker']:
        c = pg_query(f""" 
select count(*) from complaints where make = '{maker}'
and "modelYear" = '{row['Year']}'
""")
        complain = complain + c['count'][0] 

    complaints.append(complain)

car_sales['complaints'] = complaints
car_sales['percentage'] = car_sales['complaints']/car_sales['Sold Autos']
car_sales = car_sales[car_sales['percentage']>0]

unparsed = []
for _,row in car_sales.iterrows():
    for make in row['Automaker']:
        unparsed.append({
            'parent': row['Umbrella'],
            'make': make,
            'modelYear': row['Year'],
            'parent_autos_sold': row['Sold Autos'],
            'world_rank': row['World Rank'],
            'parent_modelYear_complaints': row['complaints'],
            'parent_modelYear_percentage': row['percentage']
        })

df = pd.DataFrame(unparsed)
# Save to database
db = pg_connect()
df.to_sql('car_sales',db,index=False,if_exists='replace')
db.dispose()

pg_execute("""
UPDATE car_sales
SET make = REPLACE(make, 'MERCEDES BENZ', 'MERCEDES-BENZ')
WHERE make = 'MERCEDES BENZ';
""")
