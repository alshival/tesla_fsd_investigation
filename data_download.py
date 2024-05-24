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
        makes_for_year
        makes_for_year.to_sql('makes_for_year',db,index=False,if_exists='append')
db.dispose()
# Remove duplicate rows from the table
pg_clean_table('makes_for_year')

# Create table to track model updates
if 'model_download_tracker' not in pg_tables():
    query = """
create table model_download_tracker as
select
	*,
	CURRENT_TIMESTAMP - interval '1000 years' as models_last_updated,
    0 as models_downloaded
from makes_for_year
"""
    pg_execute(query)
    print("model_download_tracker table created")

# Update model download tracker
pg_execute("""
INSERT INTO model_download_tracker
select distinct on ("modelYear",make)
	"modelYear",
	make,
	CURRENT_TIMESTAMP - interval '1000 years' as models_last_updated,
    0 as models_downloaded
from makes_for_year
where ("modelYear","make") not in (select "modelYear",make from model_download_tracker)
""")
print("model_download_tracker updated")


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

makes_for_year = pg_query("""
select distinct 
	"modelYear",
	make,
    models_last_updated,
    models_downloaded
from model_download_tracker 
where "modelYear"::int >= extract(year from current_date) 
and "modelYear"::int <= extract(year from current_date) + 1
and models_last_updated < CURRENT_DATE - INTERVAL '7 days'
union all
select
	"modelYear",
	make,
    models_last_updated,
    models_downloaded
from (
	SELECT DISTINCT
		*,
		random()
	FROM model_download_tracker
	WHERE "modelYear"::int < EXTRACT(YEAR FROM CURRENT_DATE)
	  AND "modelYear"::int >= EXTRACT(YEAR FROM CURRENT_DATE) - 5
	  AND models_last_updated < CURRENT_DATE - INTERVAL '30 days'
      AND models_downloaded = 0
	order by random() desc
	limit 500
) tbl      
""")

all_models = []
if len(makes_for_year) > 0:
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
        # Update model_download_tracker
        with db.connect() as connection:
            query = text('''
                update model_download_tracker
                set models_last_updated = current_timestamp,
                    models_downloaded = :x
                where "modelYear" = :y and make = :z
                ''')
            connection.execute(query,{'x':len(payload),'y':str(row['modelYear']),'z':row['make']})
            connection.commit()
        # Done.    
        print(f"{row['modelYear']} {row['make']} models updated: {payload.shape[0]} new models")
        time.sleep(2)
    db.dispose()
    # Clean database
    pg_clean_table('models_for_make_year')
    print(f"models_for_make_year table cleaned")

########################################
# Update Complaints
########################################
# Create table to track complaint updates
if 'complaints_download_tracker' not in pg_tables():
    query = """
create table complaints_download_tracker as
select
	*,
	CURRENT_TIMESTAMP - interval '1000 years' as complaints_last_updated,
    0 as total_complaints
from models_for_make_year
"""
    pg_execute(query)
    print("complaints_download_tracker table created")

# Update complaint download tracker
pg_execute("""
INSERT INTO complaints_download_tracker
select distinct on ("modelYear","make","model")
	"modelYear",
	make,
    "model",
	CURRENT_TIMESTAMP - interval '1000 years' as complaints_last_updated,
    0 as total_complaints
from models_for_make_year
where ("modelYear","make","model") not in (select "modelYear","make","model" from complaints_download_tracker)
""")

print("complaints_download_tracker updated")

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
                make_complaints.append(c)
        make_df = pd.DataFrame(make_complaints)
        db = pg_connect()
        if len(make_df) > 0:
            make_df.to_sql('complaints',db,index=False,if_exists='append')
        # update complaints download tracker
        with db.connect() as connection:
            query = text('''
            update complaints_download_tracker
            set complaints_last_updated = current_timestamp, total_complaints = :w
            where "modelYear" = :x and make = :y and model = :z
            ''')
            connection.execute(query,{'w':make_df.shape[0],'x': str(row['modelYear']),'y':row['make'],'z':row['model']})
            connection.commit()
        db.dispose()
        # Done.
        print(f'Complaint data for {row['modelYear']} {row['make']} {row['model']} updated: {make_df.shape[0]} total complaints')
        time.sleep(1)

make_model_year = pg_query(f""" 
(
    select 
        * 
    from complaints_download_tracker
    where make='TESLA' and "modelYear"::int >= extract(year from current_date) - 5
    and extract('days' from current_timestamp - complaints_last_updated) > 0
)
union all
(
    select
        "modelYear",
        "make",
        "model",
        "complaints_last_updated",
        "total_complaints"
    from (
        select 
            *,
            random()
        from complaints_download_tracker
        where make !='TESLA'
        and "modelYear"::int >= extract(year from current_date) - 5
        and extract('days' from current_timestamp - complaints_last_updated) > 3
    ) tbl
    order by random() limit 400
)
""")
if len(make_model_year) > 0:
    update_complaints(make_model_year)
print(make_model_year)
print(f"Stale data randomly updated.")


pg_execute("drop table if exists complaints_backup")
pg_execute("""
create table complaints_backup as
select distinct on ("odiNumber")
    *
from complaints 
""")
pg_execute("delete from complaints")
pg_execute('insert into complaints select * from complaints_backup')
pg_execute('drop table complaints_backup')

print("Complaints data table cleaned.")

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


####################
# Kagle Update
####################
import os
dataDir = 'kaggle_files/'
pg_query("select distinct on (\"odiNumber\") * from complaints").to_csv(os.path.join(dataDir,'complaints.csv'),index=False)
pg_query("""
select
	*
from models_for_make_year
where "modelYear"::int >= 2019
""").to_csv(os.path.join(dataDir,'car_models.csv'),index=False)

import subprocess

# Define the PowerShell command
command = 'kaggle datasets version -p .\\kaggle_files\\ -m "Test update"'

# Execute the PowerShell command
result = subprocess.run(["powershell", "-Command", command], capture_output=True, text=True)

# Print the output and error (if any)
print("Output:", result.stdout)
print("Error:", result.stderr)
