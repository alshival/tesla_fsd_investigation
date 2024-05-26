########################################
# Update Complaints
########################################
import pandas as pd
import requests
import time
import re
from datetime import date, datetime
from config import *
# Function to get complaints from NHTSA API
from requests.exceptions import Timeout, RequestException
import time

print('Updating complaints...')
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
            attempt += 1
            # Handle other request exceptions
            print(f"Request failed: {e}")
            time.sleep(5)
        except Exception as e:
            attempt += 1
            print(f"Request failed: {e}")
            time.sleep(5)

    # If all attempts fail, return None
    print("All attempts to contact the API have failed.")
    return None

# Function to get all Model Years
def get_complaints_model_years(retries=3, timeout=30):
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
            attempt += 1
            # Handle other request exceptions
            print(f"Request failed: {e}")
            time.sleep(5)
        except Exception as e:
            attempt += 1
            print(f"Request failed: {e}")
            time.sleep(5)

    # If all attempts fail, return an error message
    print("All attempts to contact the API have failed.")
    return "Error: All attempts to contact the API have failed."

# Fetch model years
complaints_model_years = get_complaints_model_years()

# Save model years to database
db = pg_connect()
complaints_model_years.to_sql('complaints_model_years',db,index=False,if_exists='replace')

# Function to get all Makes for the Model Year
# Function to get makes for a specific year
def get_complaints_makes_for_years(year, retries=3, timeout=30):
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
            attempt += 1
            # Handle other request exceptions
            print(f"Request failed: {e}")
            time.sleep(5)
        except Exception as e:
            attempt += 1
            print(f"Request failed: {e}")
            time.sleep(5)

    # If all attempts fail, return an error message
    print("All attempts to contact the API have failed.")
    return "Error: All attempts to contact the API have failed."


for year in complaints_model_years[complaints_model_years['modelYear'].astype(int)>=2016]['modelYear']:
    print(f'Downloading makes for year {year}')
    complaints_makes_for_years = get_complaints_makes_for_years(year)
    complaints_makes_for_years
    complaints_makes_for_years.to_sql('complaints_makes_for_years',db,index=False,if_exists='append')
db.dispose()
# Remove duplicate rows from the table
pg_clean_table('complaints_makes_for_years')

# Create table to track model updates
if 'complaints_model_download_tracker' not in pg_tables():
    query = """
create table complaints_model_download_tracker as
select
	*,
	CURRENT_TIMESTAMP - interval '1000 years' as models_last_updated,
    0 as models_downloaded
from complaints_makes_for_years
"""
    pg_execute(query)
    print("complaints_model_download_tracker table created")

# Update model download tracker
pg_execute("""
INSERT INTO complaints_model_download_tracker
select distinct on ("modelYear",make)
	"modelYear",
	make,
	CURRENT_TIMESTAMP - interval '1000 years' as models_last_updated,
    0 as models_downloaded
from complaints_makes_for_years
where ("modelYear","make") not in (select "modelYear",make from complaints_model_download_tracker)
""")
print("complaints_model_download_tracker updated")

# Function to get all Models for the Make and Model Year
def get_complaints_models(make, year, retries=3, timeout=30):
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
            # Handle timeout exception
            attempt += 1
            print(f"Attempt {attempt} timed out. Retrying...")
            time.sleep(5)  # wait before retrying
        except RequestException as e:
            attempt += 1
            # Handle other request exceptions
            print(f"Request failed: {e}")
            time.sleep(5)
        except Exception as e:
            attempt += 1
            print(f"Request failed: {e}")
            time.sleep(5)

    print("All attempts to contact the API have failed.")
    return "Error: All attempts to contact the API have failed."

complaints_makes_for_years = pg_query("""
select distinct 
	"modelYear",
	make,
    models_last_updated,
    models_downloaded
from complaints_model_download_tracker 
where "modelYear"::int >= extract(year from current_date) 
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
	FROM complaints_model_download_tracker
	WHERE "modelYear"::int < EXTRACT(YEAR FROM CURRENT_DATE)
	  AND "modelYear"::int >= EXTRACT(YEAR FROM CURRENT_DATE) - 5
	  AND models_last_updated < CURRENT_DATE - INTERVAL '15 days'
      AND models_downloaded = 0
	order by random() desc
	limit 500
) tbl      
""")

all_models = []
if len(complaints_makes_for_years) > 0:
    db = pg_connect()
    for _,row in complaints_makes_for_years.iterrows():
        print(f"Downloading {row['make']} {row['modelYear']} models")
        # Download models
        try:
            download = get_complaints_models(row['make'],str(row['modelYear']))
        except Exception as e:
            print("Download failed. Will try again next update.")
        # add to list
        print(download)
        payload = pd.DataFrame(download)
        if len(payload) > 0:
            payload.to_sql('complaints_models',db,index=False, if_exists='append')
        # Update complaints_model_download_tracker
        with db.connect() as connection:
            query = text('''
                update complaints_model_download_tracker
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
    pg_clean_table('complaints_models')
    print(f"complaints_models table cleaned")

# Create table to track complaint updates
if 'complaints_download_tracker' not in pg_tables():
    query = """
create table complaints_download_tracker as
select
	*,
	CURRENT_TIMESTAMP - interval '1000 years' as complaints_last_updated,
    0 as total_complaints
from complaints_models
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
from complaints_models
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

########################################
# Update Ratings
########################################
# Get rating years
url = 'https://api.nhtsa.gov/SafetyRatings'
response = requests.get(url).json()
rating_years = pd.DataFrame(response['Results'])
db = pg_connect()
rating_years.to_sql('ratings_years',db,index=False,if_exists='replace')
db.dispose()

# Get makes for ratings year
def get_makes_for_rating_year(year,retries=3, timeout=30):
    url = f'https://api.nhtsa.gov/SafetyRatings/modelyear/{year}'
    attempt = 0
    while attempt < retries:
            try:
                # Make the GET request to the NHTSA API with a timeout
                response = requests.get(url, timeout=timeout)
                
                # Check if the request was successful
                if response.status_code == 200:
                    # Return the JSON response
                    return response.json().get('Results', [])
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
                attempt += 1
                # Handle other request exceptions
                print(f"Request failed: {e}")
                time.sleep(5)
            except Exception as e:
                attempt += 1
                print(f"Request failed: {e}")
                time.sleep(5)

    # If all attempts fail, return None
    print("All attempts to contact the API have failed.")
    return None

# download makes for rating years
db = pg_connect()
for year in rating_years['ModelYear'][rating_years['ModelYear']>=2019]:
    temp =get_makes_for_rating_year(year)
    temp_df = pd.DataFrame(temp)
    temp_df.to_sql('ratings_makes_for_years',db,index=False,if_exists='append')
db.dispose()

pg_clean_table('ratings_makes_for_years')

# Get makes for ratings year
def get_models_for_make_rating_years(year,make,retries=3, timeout=30):
    url = f'https://api.nhtsa.gov/SafetyRatings/modelyear/{year}/make/{make}'
    attempt = 0
    while attempt < retries:
            try:
                # Make the GET request to the NHTSA API with a timeout
                response = requests.get(url, timeout=timeout)
                
                # Check if the request was successful
                if response.status_code == 200:
                    # Return the JSON response
                    return response.json().get('Results', [])
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
                attempt += 1
                # Handle other request exceptions
                print(f"Request failed: {e}")
                time.sleep(5)
            except Exception as e:
                attempt += 1
                print(f"Request failed: {e}")
                time.sleep(5)

    # If all attempts fail, return None
    print("All attempts to contact the API have failed.")
    return None
    

# Create table to track model updates
if 'ratings_model_download_tracker' not in pg_tables():
    query = """
create table ratings_model_download_tracker as
select
	*,
	CURRENT_TIMESTAMP - interval '1000 years' as models_last_updated,
    0 as models_downloaded
from ratings_makes_for_years
"""
    pg_execute(query)
    print("ratings_model_download_tracker table created")

# Update model download tracker
pg_execute("""
INSERT INTO ratings_model_download_tracker
select distinct on ("ModelYear","Make","VehicleId")
	"ModelYear",
	"Make",
    "VehicleId",
	CURRENT_TIMESTAMP - interval '1000 years' as models_last_updated,
    0 as models_downloaded
from ratings_makes_for_years
where ("ModelYear","Make","VehicleId") not in (select "ModelYear","Make","VehicleId" from ratings_model_download_tracker)
""")
print("ratings_model_download_tracker updated")

ratings_model_download_tracker = pg_query("""
select
    *
from ratings_model_download_tracker
where models_last_updated < current_date - interval '15 days'
and "ModelYear"::int >= extract(year from current_date) - 5
""")

db = pg_connect()
for _,row in ratings_model_download_tracker.iterrows():
    temp = get_models_for_make_rating_years(row['ModelYear'],row['Make'])
    temp_df = pd.DataFrame(temp)
    temp_df.to_sql('ratings_models',db,index=False,if_exists='append')
    time.sleep(1)
    with db.connect() as connection:
        query = text("""
                   update ratings_model_download_tracker
                   set models_last_updated = current_timestamp, models_downloaded = :a
                   where "ModelYear"::int = :x and "Make" = :y and "VehicleId" = :z
                   """)
        connection.execute(query,{'a':len(temp_df),'x':row['ModelYear'],'y':row['Make'],'z':row['VehicleId']})
        connection.commit()
    print(f'rating models for {row['ModelYear']} {row['Make']} updated')
db.dispose()

pg_clean_table('ratings_models')

if 'ratings_download_tracker' not in pg_tables():
    query = """
create table ratings_model_variants_download_tracker as
select
	*,
	CURRENT_TIMESTAMP - interval '1000 years' as variants_last_updated,
    0 as total_variants
from ratings_models
"""
    pg_execute(query)
    print("ratings_model_variants_download_tracker table created")

# Update complaint download tracker
pg_execute("""
INSERT INTO ratings_model_variants_download_tracker
select distinct on ("ModelYear","Make","Model","VehicleId")
	"ModelYear",
	"Make",
    "Model",
    "VehicleId",     
	CURRENT_TIMESTAMP - interval '1000 years' as variants_last_updated,
    0 as total_variants
from ratings_models
where ("ModelYear","Make","Model","VehicleId") not in (select "ModelYear","Make","Model","VehicleId" from ratings_model_variants_download_tracker)
""")

# Get makes for ratings year
def get_model_variants(year,make,model,retries=3, timeout=30):
    url = f'https://api.nhtsa.gov/SafetyRatings/modelyear/{year}/make/{make}/model/{model}'
    attempt = 0
    while attempt < retries:
            try:
                # Make the GET request to the NHTSA API with a timeout
                response = requests.get(url, timeout=timeout)
                
                # Check if the request was successful
                if response.status_code == 200:
                    # Return the JSON response
                    return response.json().get('Results', [])
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
                attempt += 1
                # Handle other request exceptions
                print(f"Request failed: {e}")
                time.sleep(5)
            except Exception as e:
                attempt += 1
                print(f"Request failed: {e}")
                time.sleep(5)

    # If all attempts fail, return None
    print("All attempts to contact the API have failed.")
    return None

if 'ratings_download_tracker' in pg_tables():
    variants_download_tracker = pg_query("""
    select
        *
    from ratings_model_variants_download_tracker
    where variants_last_updated < current_date - interval '7 days'
    and "ModelYear" >= extract(year from current_date)
    and "VehicleId" not in (select "VehicleId" from ratings_download_tracker where rated = true)
    """)
else: 
    variants_download_tracker = pg_query("""
    select
        *
    from ratings_model_variants_download_tracker
    where variants_last_updated < current_date - interval '7 days'
    and "ModelYear" >= extract(year from current_date)
    """)

db = pg_connect()
for _, row in variants_download_tracker.iterrows():
    # Download variants
    variants = get_model_variants(row['ModelYear'],row['Make'],row['Model'])
    variants_df = pd.DataFrame(variants)
    variants_df['ModelYear'] = row['ModelYear']
    variants_df['Make'] = row['Make']
    variants_df['Model'] = row['Model']
    variants_df.to_sql('ratings_models_variants',db,index=False,if_exists='append')
    # Download rating for each model variant
    with db.connect() as connection:
        query = text("""
        update ratings_model_variants_download_tracker
        set variants_last_updated = current_timestamp, total_variants = :a
        where "ModelYear" = :x and "Make" = :y and "Model" = :z and "VehicleId" = :w
        """)
        connection.execute(query,{'a':variants_df.shape[0],'x':row['ModelYear'],'y':row['Make'],'z':row['Model'],'w':row['VehicleId']})
        connection.commit()
    print(f'{row["ModelYear"]} {row['Make']} {row["Model"]} variants downloaded')
    time.sleep(1)
pg_clean_table('ratings_models_variants')

# Get makes for ratings year
def get_rating(vehicleid,retries=3, timeout=30):
    url = f'https://api.nhtsa.gov/SafetyRatings/VehicleId/{vehicleid}'
    attempt = 0
    while attempt < retries:
            try:
                # Make the GET request to the NHTSA API with a timeout
                response = requests.get(url, timeout=timeout)
                
                # Check if the request was successful
                if response.status_code == 200:
                    # Return the JSON response
                    return response.json().get('Results', [])
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
                attempt += 1
                # Handle other request exceptions
                print(f"Request failed: {e}")
                time.sleep(5)
            except Exception as e:
                attempt += 1
                print(f"Request failed: {e}")
                time.sleep(5)

    # If all attempts fail, return None
    print("All attempts to contact the API have failed.")
    return None

if 'ratings_download_tracker' not in pg_tables():
    query = """
    create table ratings_download_tracker as 
    select
        *,
        current_timestamp - interval '1000 years' as ratings_last_updated,
        false as fetched_ratings,
        false as rated
    from ratings_models_variants            
    """
    pg_execute(query)

# Update complaint download tracker
pg_execute("""
INSERT INTO ratings_download_tracker
select distinct on ("VehicleId")
	*,
	current_timestamp - interval '1000 years' as ratings_last_updated,
    false as feched_ratings,
    false as rated
from ratings_models_variants
where ("VehicleId") not in (select "VehicleId" from ratings_download_tracker)
""")

ratings_download_tracker = pg_query("""
select 
    *
from ratings_download_tracker
where fetched_ratings = false
union all
(select
    *
from ratings_download_tracker
where fetched_ratings = true
and rated = false
and ratings_last_updated < current_date - interval '14 days'
and "ModelYear"::int = extract(year from current_date)
limit 50
)
union all 
(select
    *
from ratings_download_tracker
where fetched_ratings = true
and rated = false
and "ModelYear"::int > extract(year from current_date)
)
""")

db = pg_connect()
t = pg_query("select current_timestamp")['current_timestamp'][0]
for _,row in ratings_download_tracker.iterrows():
    ratings = get_rating(row['VehicleId'])
    ratings_df = pd.DataFrame(ratings)[['OverallRating', 'OverallFrontCrashRating',
       'FrontCrashDriversideRating', 'FrontCrashPassengersideRating',
       'OverallSideCrashRating', 'SideCrashDriversideRating',
       'SideCrashPassengersideRating',
       'combinedSideBarrierAndPoleRating-Front',
       'combinedSideBarrierAndPoleRating-Rear', 'sideBarrierRating-Overall',
       'RolloverRating', 'RolloverRating2', 'RolloverPossibility',
       'RolloverPossibility2', 'dynamicTipResult', 'SidePoleCrashRating',
       'NHTSAElectronicStabilityControl', 'NHTSAForwardCollisionWarning',
       'NHTSALaneDepartureWarning', 'ModelYear', 'Make', 'Model',
       'VehicleDescription', 'VehicleId']]
    ratings_df['rating_updated_on'] = t
    ratings_df.to_sql('ratings',db,index=False,if_exists='append')
    if ratings_df['OverallRating'].iloc[0] == 'Not Rated':
        rated = False
    else:
        rated = True

    with db.connect() as connection:
        query = text("""
        update ratings_download_tracker
        set 
            ratings_last_updated = :t, 
            fetched_ratings = true, 
            rated = :r
        where  "VehicleId" = :a
        """)
        connection.execute(query,{'a':row['VehicleId'],'r':rated,'t':t})
        connection.commit()
    print(f'{row['VehicleDescription']} ratings fetched.')
    time.sleep(1)
db.dispose()
pg_execute("drop table if exists ratings_backup")
pg_execute("""
create table ratings_backup as
select distinct on ("VehicleId")
*
from ratings
order by "VehicleId", rating_updated_on desc
""")
pg_execute("delete from ratings")
pg_execute("""
insert into ratings
select * from ratings_backup
""")
pg_execute("drop table ratings_backup")
print("ratings updated")

########################################
# Update Recalls
########################################
# Function to get complaints from NHTSA API
import requests
from requests.exceptions import Timeout, RequestException
import time

# Get recall years
url = 'https://api.nhtsa.gov/products/vehicle/modelYears?issueType=r '
response = requests.get(url).json()
recalls_years = pd.DataFrame(response['results'])
db = pg_connect()
recalls_years.to_sql('recalls_years',db,index=False,if_exists='replace')
db.dispose()

# Get makes for recall year
def get_makes_for_recalls_year(year,retries=3, timeout=30):
    url = f'https://api.nhtsa.gov/products/vehicle/makes?modelYear={year}&issueType=r'
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
                attempt += 1
                # Handle other request exceptions
                print(f"Request failed: {e}")
                time.sleep(5)
            except Exception as e:
                attempt += 1
                print(f"Request failed: {e}")
                time.sleep(5)

    # If all attempts fail, return None
    print("All attempts to contact the API have failed.")
    return None

# download makes for recall years
db = pg_connect()
for year in recalls_years['modelYear'][recalls_years['modelYear'].apply(int)>=2019]:
    temp =get_makes_for_recalls_year(year)
    temp_df = pd.DataFrame(temp)
    if len(temp_df) > 0:
        temp_df.to_sql('recalls_makes_for_years',db,index=False,if_exists='append')
db.dispose()

pg_clean_table('recalls_makes_for_years')

# Get makes for recall year
def get_models_for_make_recall_years(year,make,retries=3, timeout=30):
    url = f'https://api.nhtsa.gov/products/vehicle/models?modelYear={year}&make={make}&issueType=r'
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
                attempt += 1
                # Handle other request exceptions
                print(f"Request failed: {e}")
                time.sleep(5)
            except Exception as e:
                attempt += 1
                print(f"Request failed: {e}")
                time.sleep(5)

    # If all attempts fail, return None
    print("All attempts to contact the API have failed.")
    return None
    

# Create table to track model updates
if 'recalls_model_download_tracker' not in pg_tables():
    query = """
create table recalls_model_download_tracker as
select
	*,
	CURRENT_TIMESTAMP - interval '1000 years' as models_last_updated,
    0 as models_downloaded
from recalls_makes_for_years
"""
    pg_execute(query)
    print("recalls_model_download_tracker table created")

# Update model download tracker
pg_execute("""
INSERT INTO recalls_model_download_tracker
select distinct on ("modelYear","make")
	"modelYear",
	"make",
	CURRENT_TIMESTAMP - interval '1000 years' as models_last_updated,
    0 as models_downloaded
from recalls_makes_for_years
where ("modelYear","make") not in (select "modelYear","make" from recalls_model_download_tracker)
""")
print("recalls_model_download_tracker updated")

recalls_model_download_tracker = pg_query("""
select
    *
from recalls_model_download_tracker
where models_last_updated < current_date - interval '15 days'
and "modelYear"::int >= extract(year from current_date) - 5
""")

db = pg_connect()
for _,row in recalls_model_download_tracker.iterrows():
    temp = get_models_for_make_recall_years(row['modelYear'],row['make'])
    temp_df = pd.DataFrame(temp)
    temp_df.to_sql('recalls_models',db,index=False,if_exists='append')
    time.sleep(1)
    with db.connect() as connection:
        query = text("""
                   update recalls_model_download_tracker
                   set models_last_updated = current_timestamp, models_downloaded = :a
                   where "modelYear"::int = :x and "make" = :y
                   """)
        connection.execute(query,{'a':len(temp_df),'x':row['modelYear'],'y':row['make']})
        connection.commit()
    print(f'recall models for {row['modelYear']} {row['make']} updated')
db.dispose()

pg_clean_table('recalls_models')

if 'recalls_download_tracker' not in pg_tables():
    query = """
create table recalls_download_tracker as
select
	*,
	CURRENT_TIMESTAMP - interval '1000 years' as recalls_last_updated,
    0 as total_recalls
from recalls_models
"""
    pg_execute(query)
    print("recalls_download_tracker table created")

# Update complaint download tracker
pg_execute("""
INSERT INTO recalls_download_tracker
select distinct on ("modelYear","make","model")
	"modelYear",
	"make",
    "model",
	CURRENT_TIMESTAMP - interval '1000 years' as recalls_last_updated,
    0 as total_recalls
from recalls_models
where ("modelYear","make","model") not in (select "modelYear","make","model" from recalls_download_tracker)
""")

# Get makes for recall year
def get_recalls(year,make,model,retries=3, timeout=30):
    url = f'https://api.nhtsa.gov/recalls/recallsByVehicle?make={make}&model={model}&modelYear={year}'
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
                attempt += 1
                # Handle other request exceptions
                print(f"Request failed: {e}")
                time.sleep(5)
            except Exception as e:
                attempt += 1
                print(f"Request failed: {e}")
                time.sleep(5)

    # If all attempts fail, return None
    print("All attempts to contact the API have failed.")
    return None

recalls_download_tracker = pg_query("""
select 
    *,
    random()
from recalls_download_tracker
where recalls_last_updated < current_date - interval '14 days'
and "modelYear"::int >= extract(year from current_date) - 5
order by random()
limit 750
""")

db = pg_connect()
for _, row in recalls_download_tracker.iterrows():
    temp = get_recalls(row['modelYear'],row['make'],row['model'])
    temp_df = pd.DataFrame(temp)
    missing_columns = [x for x in ['Manufacturer', 'NHTSACampaignNumber', 'parkIt', 'parkOutSide',
       'ReportReceivedDate', 'Component', 'Summary', 'Consequence', 'Remedy',
       'Notes', 'ModelYear', 'Make', 'Model', 'NHTSAActionNumber'] if x not in temp_df.columns]
    for col in missing_columns:
        temp_df[col] = None
    if len(temp_df) > 0:
        temp_df.to_sql('recalls',db,index=False,if_exists = 'append',)
    with db.connect() as connection:
        query = text("""
        update recalls_download_tracker
        set recalls_last_updated = current_timestamp, total_recalls = :t
        where "modelYear" = :x and "make" = :y and "model" = :z
        """)
        connection.execute(query,{'t':temp_df.shape[0],'x':row['modelYear'],'y':row['make'],'z':row['model']})
        connection.commit()
    print(f"Recalls for {row['modelYear']} {row['Make']} {row['Model']} updated")
    time.sleep(1)

pg_execute("drop table if exists recalls_backup")
pg_execute("""
create table recalls_backup as
select distinct on ("NHTSACampaignNumber","ModelYear","Make","Model")
*
from recalls
order by "NHTSACampaignNumber","ModelYear","Make","Model", "NHTSAActionNumber" desc
""")
pg_execute("delete from recalls")
pg_execute("""
insert into recalls
select * from recalls_backup
""")
pg_execute("drop table recalls_backup")
print("recalls updated")


########################################
# Update Investigations
########################################

import requests
import zipfile
import os

url = "https://static.nhtsa.gov/odi/ffdd/inv/FLAT_INV.zip"
metadata_url = "https://static.nhtsa.gov/odi/ffdd/inv/INV.txt"
output_path = "investigations/FLAT_INV.zip"
extracted_folder = "investigations"  # Specify the folder where you want to extract the files

# Ensure the directory exists
os.makedirs("investigations", exist_ok=True)

# Download the ZIP file
r = requests.get(url)
with open(output_path, "wb") as f:
    f.write(r.content)


print(f"File downloaded successfully to {output_path}")

# Extract the contents
with zipfile.ZipFile(output_path, "r") as zip_ref:
    zip_ref.extractall(extracted_folder)

print(f"Files extracted to {extracted_folder}")

# Download the metadata file
metadata_r = requests.get(metadata_url)
with open("investigations/INV.txt", "wb") as f:
    f.write(metadata_r.content)

print(f"Metadata file downloaded successfully")


# Read the content of INV.txt
with open("investigations/INV.txt", 'r') as f:
    metadata = f.read()

# Extract the table using regex
table_pattern = re.compile(r'Field#\s+Name\s+Type/Size\s+Description\s+------\s+----------\s+---------\s+-------------------------------\s+(.*)', re.DOTALL)
match = table_pattern.search(metadata)

if match:
    table_text = match.group(1).strip()
    lines = table_text.split("\n")
    table_data = []
    
    for line in lines:
        columns = re.split(r'\s{2,}', line.strip())  # Split by 2 or more spaces
        if len(columns) == 4:
            field_number, name, type_size, description = columns
            table_data.append({
                "Field#": field_number,
                "Name": name,
                "Type/Size": type_size,
                "Description": description
            })
    
columns = [x['Name'] for x in table_data]

import pandas as pd
from config import *
investigations = pd.read_csv('investigations/FLAT_INV.txt',sep='\t',header=None,names=columns,dtype=str)
db = pg_connect()
investigations.to_sql('investigations',db,index=False,if_exists = 'replace')
db.dispose()

########################################
# Update Kaggle
########################################
import os
from config import *

dataDir = 'kaggle_files/'
# complaints
pg_query("select distinct on (\"odiNumber\") * from complaints").to_csv(os.path.join(dataDir,'complaints.csv'),index=False)
# car models
pg_query("""
select
	*
from complaints_models
where "modelYear"::int >= 2019
""").to_csv(os.path.join(dataDir,'car_models.csv'),index=False)
# ratings
pg_query("""
select distinct on ("ModelYear","Make","Model","VehicleId")
	*
from ratings order by "ModelYear","Make","Model","VehicleId", rating_updated_on desc
""").to_csv(os.path.join(dataDir,'ratings.csv'),index=False)
# recalls
pg_query("""
select distinct on ("NHTSACampaignNumber","ReportReceivedDate","Component","ModelYear","Make","Model")
	*
from recalls
order by "NHTSACampaignNumber","ReportReceivedDate","Component","ModelYear","Make","Model", "NHTSAActionNumber" nulls last
""").to_csv(os.path.join(dataDir,'recalls.csv'),index=False)
# investigations
pg_query("""
select * from investigations
""").to_csv(os.path.join(dataDir,'investigations.csv'),index=False)

import subprocess

# Define the PowerShell command
command = 'kaggle datasets version -p .\\kaggle_files\\ -m "Daily update"'

# Execute the PowerShell command
result = subprocess.run(["powershell", "-Command", command], capture_output=True, text=True)

# Print the output and error (if any)
print("Output:", result.stdout)
print("Error:", result.stderr)