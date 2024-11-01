from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
from datetime import datetime

# Configuration de l'authentification
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
# Connexion au cluster Cassandra via Docker
cluster = Cluster(['localhost'], port=9042, auth_provider=auth_provider)
session = cluster.connect()
# Sélectionner le keyspace à utiliser
session.set_keyspace('spark_streams')

class KpiOnMyCassandraDataBase:

    @staticmethod
    def get_numbers_of_rows():
        query = "SELECT count(*) FROM created_users;"
        rows = session.execute(query)

        # get data as manipulable objects
        list_rows =  [row._asdict() for row in rows][:5]

        # dataframe with list
        dataf =  pd.DataFrame(list_rows)
        kpi_value = dataf.values[0][0]
        return kpi_value
    
    @staticmethod
    def get_numbers_of_gender(gender):
        query = f"SELECT count(*) as nbre_of_male FROM created_users WHERE gender = '{gender}';"
        rows = session.execute(query)

        # get data as manipulable objects
        list_rows =  [row._asdict() for row in rows][:5]

        # dataframe with list
        dataf =  pd.DataFrame(list_rows)
        kpi_value = dataf.values[0][0]
        return str(kpi_value)
    
    @staticmethod
    def get_max_age_registration():
        query = "SELECT MIN(registered_date) as min_age FROM created_users;"
        rows = session.execute(query)

        # get data as manipulable objects
        list_rows =  [row._asdict() for row in rows][:5]

        # dataframe with list
        dataf =  pd.DataFrame(list_rows)
        date_ = str(dataf.values[0][0])
        # convert date
        date_ = datetime.strptime(date_, "%Y-%m-%dT%H:%M:%S.%fZ")

        kpi_value =  (datetime.now() - date_).days
        return str(kpi_value)
    
    @staticmethod
    def get_min_age_registration():
        query = "SELECT MAX(registered_date) as max_age FROM created_users;"
        rows = session.execute(query)

        # get data as manipulable objects
        list_rows =  [row._asdict() for row in rows][:5]

        # dataframe with list
        dataf =  pd.DataFrame(list_rows)
        date_ = str(dataf.values[0][0])
        # convert date
        date_ = datetime.strptime(date_, "%Y-%m-%dT%H:%M:%S.%fZ")

        kpi_value =  (datetime.now() - date_).days
        return kpi_value
    
    @staticmethod
    def get_number_of_nationality(country):
        query = "SELECT address FROM created_users;"
        rows = session.execute(query)

        # get data as manipulable objects --- nationality other last 500 persons
        list_rows =  [row._asdict() for row in rows]

        # dataframe with list
        dataf =  pd.DataFrame(list_rows)['address'].tolist()
        
        # count over nationality
        count = sum(f'{country}' in address.lower() for address in dataf)
        return count
    

