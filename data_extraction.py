import database_utils
from database_utils import database_connector
import sqlalchemy
import yaml
from yaml.loader import SafeLoader
import psycopg2
from sqlalchemy.engine import URL
from sqlalchemy import inspect
import pandas as pd
import sqlalchemy as db
import tabula
from tabula import read_pdf 
from data_cleaning import data_clean
import requests
import json
import boto3
from io import BytesIO
from data_cleaning import data_clean

headers={'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

class data_extractor(database_connector):

   # function to list table names 
   def list_db_tables(self):
      self.init_db_engine()
        
      print (inspect(self.engine).get_table_names())

   def list_salesdb_tables(self):
      self.init_db_engine()
     

   # Function to extract table and open in a pandas dataframe
   def extract_rds_table(self,table_name,name_table):
      self.init_db_engine()
      self.engine = sqlalchemy.create_engine('postgresql://postgres:infernape@localhost:5432/Sales_data')

      meta_data=db.MetaData()
      #user_table=db.Table('legacy_users',meta_data,autoload=True,autoload_with=self.engine)
      self.df=pd.read_sql(f'SELECT * FROM {table_name}' ,self.connection)
      
      pd.set_option('display.expand_frame_repr', False)
      #self.upload_to_db(df,name_table)
      return self.df

   def extract_salesdb_table(self):
      self.init_db_engine()
      self.engine = sqlalchemy.create_engine('postgresql://postgres:infernape@localhost:5432/Sales_data')
      print (inspect(self.engine).get_table_names())
      df = pd.read_sql_table('dim_users_table', self.engine)
      print(df)


   
      

   def retrieve_pdf_data (self,link):
      card_list=[]

      dfs =tabula.read_pdf("card_details.pdf", pages='all')
      card_pdf=tabula.read_pdf(link, pages="all")
      for item in card_pdf:
         for info in item.values:
            card_list.append(info)
      card_df=pd.DataFrame(card_list)
      data_clean.clean_card_data(self,card_df)

      

      self.upload_to_db(card_df,'dim_card_details')



    
      #print(user_table.columns.keys())
      print((card_df))
      #print(repr(meta_data.tables['legacy_users']))
      
   def list_number_stores(self,end_point,header):
      number_of_stores=requests.get(end_point,headers=header).json()
      print(number_of_stores )
      store_number=number_of_stores['number_stores']+1
      return(store_number)

   # CCC TEST OUT THIS FUNCTION BELOW 

   def retrieve_stores_data(self,end_point,header):
      total_stores=self.list_number_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',header)
      #print(total_stores)

      #store_number=[]
      #for store_number in range(450):
      #test=pd.DataFrame()
      list=[]
      for i in range(total_stores):   
           
         stores_data=requests.get(end_point+str(i),headers=header)
         stores_json=stores_data.json()
         list.append(stores_json)
         
      
   
         #stores_object=json.loads(stores_json)
      
         #stores_pd=pd.json_normalize(stores_json)
         #stores_data_frame=[stores_pd,test]
         #stores_database=pd.concat(stores_data_frame)
      stores_pd=pd.json_normalize(list)
      #print(stores_pd)
      storess_df=data_clean.clean_store_details_table(self,stores_pd)

         #return(stores_database)
      #print(storess_df)

         
      self.upload_to_db(storess_df,'dim_store_details')

         
      #self.upload_to_db(stores_pd,'dim_store_details')
      #print(stores_data)
   

        #print(store_number)
   def extract_from_s3(self,s3_table_name,key):
      s3 = boto3.resource('s3')
      products_database = s3.Object(s3_table_name, key).get()['Body'].read()
      #products_string=str(products_database,'utf-8')

      products_df=pd.read_csv(BytesIO(products_database))
      self.upload_to_db(products_df,'dim_products')
      print(products_df)

   def extract_json_data(self,link):
      r = requests.get(link)
      json_df = pd.DataFrame(r.json())
      #self.upload_to_db(json_df,'dim_products')
      print(json_df)

   def extract_orders_data(self):
      self.extract_rds_table("orders_table",'orders_table')
      orders_df=self.df

      orders_df=data_clean.clean_orders_table(self,orders_df)

      self.upload_to_db(orders_df,'orders_table')
      #print(orders_df)
      
    
      




 
   
extract=data_extractor()
#extract.list_salesdb_tables()
#extract.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
#extract.list_number_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',headers)
extract.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/',headers)
#extract.extract_from_s3('data-handling-public','products.csv')
#extract.extract_salesdb_table()
#extract.extract_json_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
#extract.extract_orders_data()
