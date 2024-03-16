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
query = "SELECT date, SUM(new_persons_vaccinated) as `global_persons_vaccinated` FROM `bigquery-public-data.covid19_open_data.covid19_open_data`  GROUP BY date ORDER BY date "
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
# Tworzymy 2 dataframe, ponieważ w naszych analizach badalismy zsumowane dane dla kraju lub dla daty. Złączenie tych dwóch rodzajów analiz w jeden obiekt
# mogłoby zaciemnić otrzymane wyniki.
# Danych z zaszczepionymi według daty nie zapisujemy w tej części, ponieważ nie mamy z czym ich połączyć.
merged_1 = pd.merge(country_df, disease_df, on=['country_name', 'country_code'])
merged_2 = pd.merge(merged_1, deceased_df, on=['country_name', 'country_code'])
merged_df_per_country = pd.merge(merged_2, tested_df, on=['country_name', 'country_code'])
merged_df_per_country.to_csv('C:\\Users\\rober\\OneDrive\\Pulpit\\semestr 6\\BigData\\zad1\\wyniki\\merged_data_per_country.csv', index=False)
