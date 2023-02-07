# %%

import yaml
from yaml.loader import SafeLoader
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import inspect
import pg8000
import pandas as pd
class database_connector:

  
#  function to read yaml database credentials
    def read_db(self):
        with open('db_creds.yaml') as f:
            data = yaml.safe_load(f)
        return data
   

   # function to initialise database and create sqlalchemy engine
    def init_db_engine(self):
        data=self.read_db()

       
        url_object= URL.create(
            'postgresql',
            username=data['RDS_USER'],
            password=data['RDS_PASSWORD'],
            host=data['RDS_HOST'],
            database=data['RDS_DATABASE'],
        )

        self.engine= create_engine(url_object)
        self.connection=self.engine.connect()
        #return self.engine
        test_engine = sqlalchemy.create_engine('postgresql://postgres:infernape@localhost:5432/Sales_data')
        return test_engine

    def upload_to_db(self,df,table_name):
        #Create Connection
        test_engine = sqlalchemy.create_engine('postgresql://postgres:infernape@localhost:5432/Sales_data')
        df.to_sql(table_name,test_engine,if_exists='replace',index=False)
        return test_engine


        
        
# function to list table names 
    #def list_db_tables(self):
        #self.init_db_engine()
        
        #print (inspect(self.engine).get_table_names())




        





# %%
