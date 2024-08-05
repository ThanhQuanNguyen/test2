from lib2to3.fixes.fix_tuple_params import tuple_name
import pandas as pd
from dim_generate import *
import sqlite3
from sqlite3 import Cursor
from app import *

class ETL_process(ModelAbstract):
    
    def __init__(self) -> None:
        self.drop_columns = []
        self.dimension_tables = []
        self.table_name =[]
        
    def extract(self):
        print('Extracting data from csv file, please wait a bit.')
        self.fact_table = df
        print('Extracting successfully!')
    
    def transform(self):
        # transform data types
        self.fact_table [['avg_price_per_room']] = self.fact_table[['avg_price_per_room']].astype(float)
        int_cols = ['no_of_adults', 'no_of_children', 'no_of_weekend_nights', 'no_of_week_nights', 'required_car_parking_space',
                    'repeated_guest', 'no_of_previous_cancellations', 'no_of_previous_bookings_not_canceled', 'no_of_special_requests']
        self.fact_table[int_cols] = self.fact_table[int_cols].astype(int)
        self.fact_table[['type_of_meal_plan','room_type_reserved','booking_status']] = self.fact_table[['type_of_meal_plan','room_type_reserved','booking_status']].astype(str)
        
        # fetch booking_details dimension table
        dim_booking = dim_booking_details()
        self.drop_columns += dim_booking.columns
        self.dimension_tables.append(dim_booking)
        
        # fetch price dimension table
        dim_price = dimPrice()
        self.drop_columns += dim_price.columns
        self.dimension_tables.append(dim_price)
        
        # fetch history dimension table
        dim_history = dimHistory()
        self.drop_columns += dim_history.columns
        self.dimension_tables.append(dim_history) 
        
        # fetch properties dimension table
        dim_propeties = dimProperties()
        self.drop_columns += dim_propeties.columns
        self.dimension_tables.append(dim_propeties)
        
        # fetch request dimension table
        dim_request = dimNo_request()
        self.drop_columns += dim_request.columns
        self.dimension_tables.append(dim_request)
    
        # fetch status dimension table
        dim_status = dimStatus()
        self.drop_columns += dim_status.columns
        self.dimension_tables.append(dim_status)
        
        # edit date format
        df.loc[(df['arrival_month'] == 2) & (df['arrival_date'] == 29), 'arrival_date'] = 28
        df['date'] = pd.to_datetime(df[['arrival_year', 'arrival_month', 'arrival_date']].rename(columns={
        'arrival_year': 'year', 'arrival_month': 'month', 'arrival_date': 'day'})).dt.strftime('%Y-%m-%d') 
                                                    
        # merge table to generate a Fact Table and Dimension Tables
        for dim in self.dimension_tables:
            self.fact_table = pd.merge(self.fact_table, dim.dimension_table, on=dim.columns, how ='left', suffixes=('_x2','_x3'))
        self.fact_table = self.fact_table.drop(columns=self.drop_columns)
        self.fact_table = self.fact_table.drop(columns=['arrival_year', 'arrival_month', 'arrival_date', 'market_segment_type', 'lead_time'])   
        
        # merge dim_booking and dim_price
        dim_price.dimension_table = pd.merge(dim_price.dimension_table, dim_booking.dimension_table, on= ['avg_price_per_room'], how='left')
        dim_price.dimension_table = dim_price.dimension_table.drop(columns= ['no_of_adults', 'no_of_children'])
        dim_price.dimension_table = dim_price.dimension_table.drop_duplicates()
        dim_booking.dimension_table = dim_booking.dimension_table.drop(columns= 'avg_price_per_room')
        
        print('All tables was created successfully!')
        
        # convert every dimension tables and a fact table to csv file
        if not os.path.exists('data'):
            os.makedirs('data')
        
        dim_booking.dimension_table.to_csv('data/dim_booking.csv')
        dim_price.dimension_table.to_csv('data/dim_price.csv')
        dim_history.dimension_table.to_csv('data/dim_history.csv')
        dim_propeties.dimension_table.to_csv('data/dim_properties.csv')
        dim_request.dimension_table.to_csv('data/dim_request.csv')
        dim_status.dimension_table.to_csv('data/dim_status.csv')
        self.fact_table.to_csv("data/fact_table.csv")
        
        print('All tables was successfully converted to csv file!')
        print('Transforming successfully!')
        
    def load_data_to_sqlite(self):
        # create the connection to sqlite
        connection = sqlite3.connect('booking.db')
        connection = sqlite3.connect('booking.db', check_same_thread=False)
        
        
        # adding the name of csv_file into a list
        list_name = ['dim_booking', 'dim_history', 'dim_price', 'dim_properties', 'dim_request',
                     'dim_status']
        
        for dim_var, dim_database in zip(self.dimension_tables, list_name):
            dim_var = pd.read_csv(f'data/{dim_database}.csv', index_col=0)
            if 'Unnamed' in dim_var.columns:
                dim_var = dim_var.drop(columns=['Unnamed'])
            dim_var.to_sql(dim_database, connection, if_exists = 'append')
            print(f'Complete uploading {dim_database}')
        
        
        # add fact_table to the database
        fact_table = pd.read_csv('data/fact_table.csv', index_col=0)
        if 'Unnamed' in fact_table.columns:
            fact_table = fact_table.drop(columns=['Unnamed'])
        fact_table.to_sql('days_in_fact_table', connection, if_exists='append')

        print('Load successfully! Check your SQLite.')
        
        
        
      
       
        