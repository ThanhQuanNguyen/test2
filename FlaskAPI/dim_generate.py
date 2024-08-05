import pandas as pd
import sqlite3


df = pd.read_csv("Hotel Reservations.csv")
connection = sqlite3.connect('booking.db')

class ModelAbstract():
    def __init__(self):
        self.column = None
        self.dimension_table = None
        self.name = None
        self.table_name = None
    
    def dimension_generate(self, name:str, primary:str, columns:list):
        dim = df[columns]
        dim = dim.drop_duplicates()
        self.table_name = []
        #create Primary Key for dimension table
        dim[primary] = range(1001, len(dim)+1001)
        self.table_name.append(self.name)
        self.dimension_table = dim
        self.name = name
        self.columns = columns
        
class dim_booking_details(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generate('dim_booking','booking_details_id',['no_of_adults','no_of_children','avg_price_per_room'])

class dimPrice(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generate('dim_price', 'price_id', ['avg_price_per_room'])

class dimProperties(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generate('dim_properties', 'properties_id',['type_of_meal_plan', 'required_car_parking_space', 'room_type_reserved'])

class dimHistory(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generate('dim_history', 'history_id',['repeated_guest', 'no_of_previous_cancellations', 'no_of_previous_bookings_not_canceled'])

class dimNo_request(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generate('dim_request', 'no_request_id',['no_of_special_requests'])

class dimStatus(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generate('dim_status', 'status_id', ['booking_status'])
    
        