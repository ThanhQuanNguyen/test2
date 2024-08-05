import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
# manipulate data by the sql query
def create_data():
    conn = sqlite3.connect('booking.db')
    cur = conn.cursor()
    sql_query ='''SELECT DISTINCT	
                    booking_id, 
                    no_of_week_nights, 
                    no_of_weekend_nights, 
                    no_of_adults, no_of_children, 
                    avg_price_per_room, 
                    date 
                    from (days_in_fact_table ft JOIN dim_price on ft.price_id = dim_price.price_id 
                    JOIN dim_booking on ft.booking_details_id = dim_booking.booking_details_id)'''
    cur.execute(sql_query)
    columns = [column[0] for column in cur.description]  # Get the column names
    result = cur.fetchall()
    data = [dict(zip(columns, row)) for row in result]
    df = pd.DataFrame(data, columns=columns)
    conn.close()
    return(df)

# manipulate data by the sql query
def roomtype_overview():
    conn = sqlite3.connect('booking.db')
    cur = conn.cursor()
    sql_query = '''SELECT ft.booking_id, 
                        dim_properties.room_type_reserved 
                    from (days_in_fact_table ft
                    JOIN dim_properties on dim_properties.properties_id = ft.properties_id)'''

    cur.execute(sql_query)
    columns = [column[0] for column in cur.description]  # Get the column names
    result = cur.fetchall()
    data = [dict(zip(columns, row)) for row in result]
    df = pd.DataFrame(data, columns=columns)
    conn.close()
    return(df)

# plot the charts by matplotlib
def generate_plot1():
    conn = sqlite3.connect('booking.db')
    cur = conn.cursor()
    query = '''
    SELECT 
        COUNT(CASE WHEN no_of_children = 0 THEN booking_id END) AS family_without_child,
        COUNT(CASE WHEN no_of_children > 0 THEN booking_id END) AS family_with_child
    FROM 
        days_in_fact_table 
    JOIN 
        dim_booking 
    ON 
        days_in_fact_table.booking_details_id = dim_booking.booking_details_id;
    '''
    cur.execute(query)
    columns = [column[0] for column in cur.description]  # Get the column names
    result = cur.fetchone()
    conn.close()
    
    plt.figure()
    plt.pie(result, labels=columns, autopct='%.2f%%')
    plt.title("Customer overview")
    
    plot_path = 'plot/customer_overview.png'
    if not os.path.exists('plot'):
        os.makedirs('plot')
    plt.savefig(plot_path)
    plt.close()
    
    return plot_path

# plot the charts by matplotlib
def generated_plot2():
    conn = sqlite3.connect('booking.db')
    cur = conn.cursor()
    cur2 = conn.cursor()
    query = '''SELECT  
                room_type_reserved, 
                count(*) quantity
                    from (days_in_fact_table ft
                    JOIN dim_properties on dim_properties.properties_id = ft.properties_id)
			        GROUP BY room_type_reserved'''

    cur.execute(query)  
    result = cur.fetchall()
    columns = [column[0] for column in result ]
    values = [column[1] for column in result]
    plt.rcParams['font.size'] = 6
    barchart = plt.bar(columns, values, color ='r', width=0.8)
    plt.bar_label(barchart, labels=values, label_type='edge')
    plt.title("Room_type overview")
    plot_path = 'plot/room_overview.png'
    if not os.path.exists('plot'):
        os.makedirs('plot')
    plt.savefig(plot_path)
    plt.close()
    return plot_path
    


    