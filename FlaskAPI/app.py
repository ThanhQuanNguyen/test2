import sqlite3
from flask import Flask, jsonify, request,render_template, redirect, url_for, session
import os
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine, Column, String, Integer, Float, Date, VARCHAR,ForeignKey, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from flask_jwt_extended import create_access_token, JWTManager, jwt_required
import pandas as pd
from data_manipulate import *
import matplotlib
matplotlib.use('Agg')


app = Flask(__name__, template_folder= 'templates', static_folder='plot')
base_directory = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(base_directory, 'booking.db')
app.config['JWT_SECRET_KEY'] = 'super_secret' 

db = SQLAlchemy(app)
jwt = JWTManager(app)


# Initialize the FLASK command to create the database
@app.cli.command('db_create')
def create_database():
    db.create_all()
    print('Creating database successfully!')

# Initialize the FLASK command to drop the database
@app.cli.command('db_drop')
def drop_database():
    db.drop_all()
    print('Dropping database successfully')

# Initialize the FLASK command to add new records into the database    
@app.cli.command('db_seed')
def db_seed():
    manager1 = user_account(username = 'managerTom123',
                            password = 'manager1')
    
    manager2 = user_account(username = 'managerJim234',
                            password = 'manager2')
    db.session.add(manager1)
    db.session.add(manager2)
    db.session.commit()
    print('Seeding data successfully!')

# The first endpoint of the webpage
@app.route("/")
def saysomething():
    return render_template('first_endpoint.html')

# The /login endpoint of the webpage
@app.route('/login', methods =['GET','POST'])
def login():
    if request.method == 'GET':
        return render_template('login.html')
    elif request.method == 'POST': 
        login_username = request.form['username']
        login_password = request.form['password']
    
        test = user_account.query.filter_by(username = login_username, password = login_password).first()
        if test:
            access_token = create_access_token(identity= login_username)
            return jsonify(message = 'Login successfully!', access_token = access_token)
        else:
            return jsonify(message ='The username or password does not exist. Please try again')

# The /home endpoint of the webpage
@app.route('/home', methods =['GET'])
def home_page():
    return render_template('home_page.html')

# The /data_overview endpoint of the webpage
@app.route('/data_overview', methods =['GET'])
def data_overview():
    df = create_data()
    plot_url = generate_plot1()
    return render_template('data.html', plot_url = plot_url , tables=[df.to_html(classes='dataframe', index=False)], titles=df.columns.values)

# The /room_overview endpoint of the webpage
@app.route('/room_overview', methods =['GET'])
def room_overview():
    df = roomtype_overview()
    plot_url = generated_plot2()
    return render_template('room_data.html', plot_url= plot_url, tables=[df.to_html(classes='dataframe', index=False)], titles=df.columns.values)

@app.route('/dashboard_data', methods =['GET'])
def dashboard_data():
    conn = sqlite3.connect('booking.db')
    cur = conn.cursor()
    sql_query ='''SELECT DISTINCT 
                            booking_id,
                            type_of_meal_plan,
                            required_car_parking_space,
                            room_type_reserved,
                            date 
                    FROM days_in_fact_table ft
                    JOIN dim_properties on ft.properties_id = dim_properties.properties_id
                '''
    cur.execute(sql_query)
    columns = [column[0] for column in cur.description]  # Get the column names
    result = cur.fetchall()
    data = [dict(zip(columns, row)) for row in result]
    return jsonify(data)

# Create relational database with some tables below
class Booking_Details (db.Model):
    __tablename__ = 'dim_booking'
    index = Column(Integer)
    booking_details_id = Column(Integer, primary_key=True)
    no_of_adults = Column(Integer)
    no_of_children = Column(Integer)
    db.relationship('dim_price')
    
class Price (db.Model):
    __tablename__ ='dim_price'
    index = Column(Integer)
    price_id = Column(Integer)
    avg_price_per_room = Column(Integer)
    booking_details_id = Column (Integer, ForeignKey(Booking_Details.booking_details_id))
    
    __table_args__ = (
        PrimaryKeyConstraint('price_id', 'booking_details_id'),
    )

class History(db.Model):
    __tablename__ ='dim_history'
    index = Column(Integer)
    history_id = Column(Integer, primary_key=True)
    repeated_guest = Column(Integer)
    no_of_previous_cancellations = Column(Integer)
    no_of_previous_bookings_not_canceled = Column(Integer)

class properties(db.Model):
    __tablename__ ='dim_properties'
    index = Column(Integer)
    properties_id = Column(Integer, primary_key=True)
    type_of_meal_plan = Column(Integer)
    required_car_parking_space = Column(Integer)
    room_type_reserved = Column(Integer)
    
class status (db.Model):
    __tablename__ ='dim_status'
    index = Column(Integer)
    index =Column(Integer)
    status_id = Column(Integer, primary_key=True)
    booking_status = Column(String)
    
class no_request(db.Model):
    __tablename__ ='dim_request'
    index = Column(Integer)
    no_request_id = Column(Integer, primary_key=True)
    special_request = Column(String)
    no_of_special_requests = Column(String)

class days_in_fact_table(db.Model):
    __tablename__ ='days_in_fact_table'
    index = Column(Integer)
    booking_id = Column(VARCHAR, primary_key=True)
    booking_details_id = Column(Integer, ForeignKey(Booking_Details.booking_details_id))
    price_id = Column(Integer, ForeignKey(Price.price_id))
    history_id = Column(Integer, ForeignKey(History.history_id))
    properties_id = Column(Integer, ForeignKey(properties.properties_id))
    status_id = Column(Integer, ForeignKey(status.status_id))
    no_request_id = Column(Integer, ForeignKey(no_request.no_request_id))
    no_of_week_nights = Column(Integer)
    no_of_weekend_nights = Column(Integer)
    date = Column(Date)
    #create relationship
    db.relationship('dim_booking', back_populates = 'days_in_fact_table')
    db.relationship('dim_price', back_populates = 'days_in_fact_table')
    db.relationship('dim_history', back_populates = 'days_in_fact_table')
    db.relationship('dim_properties', back_populates = 'days_in_fact_table')
    db.relationship('dim_status', back_populates = 'days_in_fact_table')

class user_account(db.Model):
    __name__ = "user_account"
    username = Column(VARCHAR, primary_key=True)
    password = Column(VARCHAR) 
    

if __name__ == '__main__':
    app.run()