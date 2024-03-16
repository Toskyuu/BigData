import os
import pandas as pd
from google.cloud import bigquery
# część 1
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\windy-access-416710-5959edbfa345.json"  # lokalizacja pobranego klucza z punktu 1.4.
client = bigquery.Client()

# # część 2
# query = "SELECT * FROM `bigquery-public-data.covid19_open_data.covid19_open_data` LIMIT 10"
# query_job = client.query(query)
# query_result = query_job.result()
# df = query_result.to_dataframe()
# df.to_csv('C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\zad1\\wyniki\\dane.csv', index=False)

#część 4
#1
query = "SELECT DISTINCT country_code, country_name FROM `bigquery-public-data.covid19_open_data.covid19_open_data`"
query_country = client.query(query)
query_country_result = query_country.result()
country_df = query_country_result.to_dataframe()
country_df.dropna(inplace=True)
country_df.to_csv('C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\zad1\\wyniki\\country_data.csv', index=False)

#2
query = "SELECT country_code, country_name, SUM(new_confirmed) as `total_confirmed_cases_per_country` FROM `bigquery-public-data.covid19_open_data.covid19_open_data`  GROUP BY country_name, country_code ORDER BY country_name "
query_disease = client.query(query)
query_disease_result = query_disease.result()
disease_df = query_disease_result.to_dataframe()
disease_df.dropna(inplace=True)
disease_df.to_csv('C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\zad1\\wyniki\\disease_data.csv', index=False)

3
query = "SELECT country_code, country_name, SUM(new_deceased) as `total_deceased_per_country` FROM `bigquery-public-data.covid19_open_data.covid19_open_data`  GROUP BY country_name, country_code ORDER BY country_name "
query_deceased = client.query(query)
query_deceased_result = query_deceased.result()
deceased_df = query_deceased_result.to_dataframe()
deceased_df.dropna(inplace=True)
deceased_df.to_csv('C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\zad1\\wyniki\\deceased_data.csv', index=False)

#4
query = "SELECT country_code, country_name, SUM(new_persons_vaccinated) as `global_persons_vaccinated` FROM `bigquery-public-data.covid19_open_data.covid19_open_data`  GROUP BY country_code, country_name ORDER BY country_name "
query_vaccine = client.query(query)
query_vaccine_result = query_vaccine.result()
vaccine_df = query_vaccine_result.to_dataframe()
vaccine_df.dropna(inplace=True)
vaccine_df.to_csv('C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\zad1\\wyniki\\vaccine_data.csv', index=False)

#5 Liczba wykonanych testów na całym świecie
query = "SELECT country_code, country_name, SUM(cast(new_tested as integer)) as `total_tested_person_per_country` FROM `bigquery-public-data.covid19_open_data.covid19_open_data`  GROUP BY country_name, country_code ORDER BY country_name "
query_tested = client.query(query)
query_tested_result = query_tested.result()
tested_df = query_tested_result.to_dataframe()
tested_df.dropna(inplace=True)
tested_df.to_csv('C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\zad1\\wyniki\\tested_data.csv', index=False)

# część 5
merged_country_disease = pd.merge(country_df, disease_df, on=['country_name', 'country_code'])
merged_country_disease_deceased = pd.merge(merged_country_disease, deceased_df, on=['country_name', 'country_code'])
merged_country_disease_deceased_vaccine = pd.merge(merged_country_disease_deceased, vaccine_df, on=['country_name', 'country_code'])
merged_df_per_country = pd.merge(merged_country_disease_deceased_vaccine, tested_df, on=['country_name', 'country_code'])
merged_df_per_country.to_csv('C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\zad1\\wyniki\\part_5_merged.csv', index=False)

# część 6
world_countries_df = pd.read_csv('C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\zad1\\dane_z_wikamp\\world_countries.csv')
gdp_df = pd.read_csv('C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\zad1\\dane_z_wikamp\\gdp.csv')

merged_df_per_country_world_countries = pd.merge(merged_df_per_country, world_countries_df, left_on='country_name', right_on='Country/Territory')
merged_df_per_country_world_countries.drop('Country/Territory', axis=1, inplace=True)
merged_df_per_country_world_countries.to_csv('C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\zad1\\wyniki\\part_6_05_merged.csv', index=False)


# to nie działa i chuj xD, bo dane są jeszcze podzielone na lata różne i nie wiem jak to sensownie zmergować :)
# merged_df_per_country_world_countries_gdp = pd.merge(merged_df_per_country_world_countries, gdp_df, left_on=['country_name', 'country_code'], right_on=["Country Name","Country Code"])
# merged_df_per_country_world_countries_gdp.to_csv('C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\zad1\\wyniki\\part_6_merged.csv', index=False)
