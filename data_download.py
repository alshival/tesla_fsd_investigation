import pandas as pd
import requests
import time
from datetime import date, datetime

from config import *

########################################
# Main function to download complaints
########################################
# Function to get complaints from NHTSA API
def get_complaints(make, model, model_year):
    # Construct the API URL
    url = f"https://api.nhtsa.gov/complaints/complaintsByVehicle?make={make}&model={model}&modelYear={model_year}"
    
    # Make the GET request to the NHTSA API
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Return the JSON response
        return response.json()['results']
    elif response.status_code == 400:
        return None
    else:
        # Return an error message
        return None

########################################
# Update Model Years
########################################
# Function to get all Model Years
def get_model_years():
    url = "https://api.nhtsa.gov/products/vehicle/modelYears?issueType=c"
    response = requests.get(url)
    if response.status_code == 200:
        return pd.DataFrame(response.json()['results'])
    else:
        return f"Error: {response.status_code}"


# Fetch model years
model_years = get_model_years()

# Save model years to database
db = pg_connect()
model_years.to_sql('model_years',db,index=False,if_exists='replace')

########################################
# Update Makes
########################################
# Function to get all Makes for the Model Year
def get_makes_for_year(year):
    url = f"https://api.nhtsa.gov/products/vehicle/makes?modelYear={year}&issueType=c"
    response = requests.get(url)
    if response.status_code == 200:
        return pd.DataFrame(response.json()['results'])
    else:
        return f"Error: {response.status_code}"

db = pg_connect()
if 'makes_for_year' in pg_tables():
    years = pg_query('select distinct "modelYear" from makes_for_year')['modelYear'].to_list()
else:
    years = []

for year in model_years[model_years['modelYear'].astype(int)>=2016]['modelYear']:
    if (year >= str(date.today().year)) | (year not in years):
        print(f'Downloading makes for year {year}')
        makes_for_year = get_makes_for_year(year)
        makes_for_year['updated_on'] = date.today()
        makes_for_year.to_sql('makes_for_year',db,index=False,if_exists='replace')

db.dispose()
# Remove duplicate rows from the table
pg_clean_table('makes_for_year')

########################################
# Update Models
########################################
# Function to get all Models for the Make and Model Year
def get_models_for_make_year(make, year):
    url = f"https://api.nhtsa.gov/products/vehicle/models?modelYear={year}&make={make}&issueType=c"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['results']
    else:
        return f"Error: {response.status_code}"
    
if 'models_for_make_year' in pg_tables():
    makes_for_year  = pg_query(""" 
    select distinct 
        "modelYear",
        make
    from makes_for_year 
    where "modelYear"::int >= EXTRACT(YEAR from CURRENT_DATE)::INT 
    and make||"modelYear" in (
        select make||"modelYear" from models_for_make_year
        where CURRENT_DATE - updated_on::Date >7
    )
        """)
else:
    makes_for_year  = pg_query(""" 
    select distinct 
        "modelYear",
        make
    from makes_for_year 
    where "modelYear"::int >= 2016
        """)

all_models = []
if len(makes_for_year) > 0:
    sample_size = min(500,len(makes_for_year))
    makes_for_year = makes_for_year.sample(sample_size)
    for _,row in makes_for_year.iterrows():
        print(f"Downloading {row['make']} {row['modelYear']} models")
        # Download models
        download = get_models_for_make_year(row['make'],str(row['modelYear']))
        # add to list
        print(download)
        all_models.extend(download)
        time.sleep(.5)
if len(all_models) > 0:
    df = pd.DataFrame(all_models)
    # Save to database
    db = pg_connect()
    df['updated_on'] = date.today()
    df.to_sql('models_for_make_year',db,index=False,if_exists='append')
    pg_clean_table('models_for_make_year')
########################################
# Update Complaints
########################################
def update_complaints(make_model_year):
    all_complaints = []
    for _,row in make_model_year.iterrows():
        print(f'Downloading data for {row['make']} {row['modelYear']} {row['model']}')
        complaints = get_complaints(row['make'],row['model'],row['modelYear'])
        if complaints:
            for c in complaints:
                c['make'] = row['make']
                c['model'] = row['model']
                c['modelYear'] = row['modelYear']
                c['products'] = json.dumps(c['products'])
                c['updated_on'] = date.today()
                all_complaints.append(c)
        time.sleep(.5)

    df = pd.DataFrame(all_complaints)
    df.to_pickle("all_complaints.pkl")

    db = pg_connect()
    # add data to database:
    df.to_sql('complaints',db,index=False,if_exists='append')
    #pg_clean_table('complaints')

########################################
# Update Tesla
########################################
make_model_year = pg_query(""" 
select distinct
    models_for_make_year.make,models_for_make_year."modelYear",models_for_make_year.model
from models_for_make_year
join (select distinct make, model, "modelYear", updated_on from complaints) last_update
    on last_update.make = models_for_make_year.make
    and last_update.model = models_for_make_year.model
    and last_update."modelYear" = models_for_make_year."modelYear"
where models_for_make_year."modelYear" >= '2016'
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
select distinct
    models_for_make_year.make,models_for_make_year."modelYear",models_for_make_year.model
from models_for_make_year
join (select distinct make, model, "modelYear", updated_on from complaints) last_update
    on last_update.make = models_for_make_year.make
    and last_update.model = models_for_make_year.model
    and last_update."modelYear" = models_for_make_year."modelYear"
where models_for_make_year."modelYear"::int >= EXTRACT(YEAR from current_date) - 6
and (CURRENT_DATE - last_update.updated_on::Date) > 5
""")
sample_size = min(500,len(make_model_year))
make_model_year = make_model_year.sample(sample_size)
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
