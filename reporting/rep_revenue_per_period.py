#!/usr/bin/env python
# coding: utf-8

# # Setup - Install Libraries

# In[ ]:


# Run the following commands once, in order to install libraries - DO NOT Uncomment this line.

# Uncomment below lines

# !pip3 install --upgrade pip
# !pip3 install google-cloud-bigquery
# !pip3 install pandas-gbq -U
# !pip3 install db-dtypes
# !pip3 install packaging --upgrade


# # Import libraries

# In[2]:


# Import libraries
from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
import os

print('Libraries imported successfully')


# In[3]:


# Set the environment variable for Google Cloud credentials
# Place the path in which the .json file is located.

# Example (if .json is located in the same directory with the notebook)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "at-arch-416714-6f9900ec7.json"

# -- YOUR CODE GOES BELOW THIS LINE
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/LoukasA/scripts/robust-arcadia-453221-u5-39c3cc6a4e86.json" # Edit path
# -- YOUR CODE GOES ABOVE THIS LINE


# In[5]:


# Set your Google Cloud project ID and BigQuery dataset details

# -- YOUR CODE GOES BELOW THIS

project_id = 'robust-arcadia-453221-u5' # Edit with your project id
dataset_id = 'reporting_db' # Modify the necessary schema name: staging_db, reporting_db etc.
table_id = 'rep_revenue_per_period' # Modify the necessary table name: stg_customer, stg_city etc.

# -- YOUR CODE GOES ABOVE THIS LINE


# # SQL Query

# In[6]:


# Create a BigQuery client
client = bigquery.Client(project=project_id)

# -- YOUR CODE GOES BELOW THIS LINE

# Define your SQL query here
query = """
with films as(
  select *
  from robust-arcadia-453221-u5.staging_db.stg_film
)

, inventory as(
  select *
  from robust-arcadia-453221-u5.staging_db.stg_inventory
)

, rentals as(
  select *
  from robust-arcadia-453221-u5.staging_db.stg_rental
)

, film_inclusion as(
  select
    rentals.rental_id,
    films.film_id,
    films.film_title,
    film_rental_duration
  from rentals
  left join inventory
    on rentals.inventory_id = inventory.inventory_id
  left join films
    on inventory.film_id = films.film_id
  where films.film_title != 'GOODFELLAS SALUTE'
)

, payments as(
  select
    rental_id,
    payment_date,
    payment_amount
  from robust-arcadia-453221-u5.staging_db.stg_payment
)

, payments_adj as(
  select
    payments.rental_id,
    payments.payment_date,
    payments.payment_amount
  from payments
  inner join film_inclusion
    on payments.rental_id = film_inclusion.rental_id
)

, reporting_dates as(
  select *
  from robust-arcadia-453221-u5.reporting_db.reporting_periods_table
  where reporting_period in ('Day','Month','Year')
)

, revenue_per_period as(
  select
    'Day' as reporting_period
    , date_trunc(payments_adj.payment_date, day) as reporting_date
    , sum(payment_amount) as total_revenue
  from payments_adj
  group by 1, 2

  union all

  select
    'Month' as reporting_period
    , date_trunc(payments_adj.payment_date, month) as reporting_date
    , sum(payment_amount) as total_revenue
  from payments_adj
  group by 1, 2

  union all

  select
    'Year' as reporting_period
    , date_trunc(payments_adj.payment_date, year) as reporting_date
    , sum(payment_amount) as total_revenue
  from payments_adj
  group by 1, 2
)

, final as(
  select
    reporting_dates.reporting_period
   , reporting_dates.reporting_date 
   , coalesce(revenue_per_period.total_revenue, 0) as total_revenue
  from reporting_dates
  left join revenue_per_period
    on reporting_dates.reporting_period = revenue_per_period.reporting_period
    and reporting_dates.reporting_date = revenue_per_period.reporting_date
    where reporting_dates.reporting_period = 'Day'
  
  union all

  select
    reporting_dates.reporting_period
   , reporting_dates.reporting_date 
   , coalesce(revenue_per_period.total_revenue, 0) as total_revenue
  from reporting_dates
  left join revenue_per_period
    on reporting_dates.reporting_period = revenue_per_period.reporting_period
    and reporting_dates.reporting_date = revenue_per_period.reporting_date
    where reporting_dates.reporting_period = 'Month'

  union all

  select
    reporting_dates.reporting_period
   , reporting_dates.reporting_date 
   , coalesce(revenue_per_period.total_revenue, 0) as total_revenue
  from reporting_dates
  left join revenue_per_period
    on reporting_dates.reporting_period = revenue_per_period.reporting_period
    and reporting_dates.reporting_date = revenue_per_period.reporting_date
    where reporting_dates.reporting_period = 'Year'
)

select *
from final
"""

# -- YOUR CODE GOES ABOVE THIS LINE

# Execute the query and store the result in a dataframe
df = client.query(query).to_dataframe()

# Explore some records
df.head()


# # Write to BigQuery

# In[7]:


# Define the full table ID
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

# -- YOUR CODE GOES BELOW THIS LINE
# Define table schema based on the project description

schema = [
    bigquery.SchemaField('reporting_period', 'STRING'),
    bigquery.SchemaField('reporting_date', 'DATE'),
    bigquery.SchemaField('total_revenue', 'NUMERIC'),
    ]

# -- YOUR CODE GOES ABOVE THIS LINE


# In[8]:


# Create a BigQuery client
client = bigquery.Client(project=project_id)

# Check if the table exists
def table_exists(client, full_table_id):
    try:
        client.get_table(full_table_id)
        return True
    except Exception:
        return False

# Write the dataframe to the table (overwrite if it exists, create if it doesn't)
if table_exists(client, full_table_id):
    # If the table exists, overwrite it
    destination_table = f"{dataset_id}.{table_id}"
    # Write the dataframe to the table (overwrite if it exists)
    to_gbq(df, destination_table, project_id=project_id, if_exists='replace')
    print(f"Table {full_table_id} exists. Overwritten.")
else:
    # If the table does not exist, create it
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Table {full_table_id} did not exist. Created and data loaded.")


# In[ ]:




