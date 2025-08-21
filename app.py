from flask import Flask, render_template, jsonify
from clickhouse_driver import Client
import pandas as pd
from datetime import datetime, timedelta

app = Flask(__name__)

# ClickHouse connection
def get_clickhouse_client():
    return Client(
        host='localhost',
        port=9000,
        user='default',
        password='',
        database='default'
    )

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/trips_by_hour')
def trips_by_hour():
    client = get_clickhouse_client()
    
    query = """
    SELECT 
        toHour(pickup_datetime) as hour,
        count(*) as trip_count
    FROM trips 
    WHERE pickup_datetime >= '2015-01-01'
    GROUP BY hour
    ORDER BY hour
    """
    
    result = client.execute(query)
    df = pd.DataFrame(result, columns=['hour', 'trip_count'])
    
    return jsonify({
        'hours': df['hour'].tolist(),
        'counts': df['trip_count'].tolist()
    })

@app.route('/api/trips_by_day')
def trips_by_day():
    client = get_clickhouse_client()
    
    query = """
    SELECT 
        toDate(pickup_datetime) as day,
        count(*) as trip_count
    FROM trips 
    WHERE pickup_datetime >= '2015-01-01'
    GROUP BY day
    ORDER BY day
    LIMIT 30
    """
    
    result = client.execute(query)
    df = pd.DataFrame(result, columns=['day', 'trip_count'])
    
    return jsonify({
        'days': df['day'].astype(str).tolist(),
        'counts': df['trip_count'].tolist()
    })

@app.route('/api/top_locations')
def top_locations():
    client = get_clickhouse_client()
    
    query = """
    SELECT 
        pickup_zone,
        count(*) as trip_count
    FROM trips 
    WHERE pickup_datetime >= '2015-01-01'
    GROUP BY pickup_zone
    ORDER BY trip_count DESC
    LIMIT 10
    """
    
    result = client.execute(query)
    df = pd.DataFrame(result, columns=['location', 'trip_count'])
    
    return jsonify({
        'locations': df['location'].tolist(),
        'counts': df['trip_count'].tolist()
    })

@app.route('/api/avg_fare_by_hour')
def avg_fare_by_hour():
    client = get_clickhouse_client()
    
    query = """
    SELECT 
        toHour(pickup_datetime) as hour,
        avg(fare_amount) as avg_fare
    FROM trips 
    WHERE pickup_datetime >= '2015-01-01'
    GROUP BY hour
    ORDER BY hour
    """
    
    result = client.execute(query)
    df = pd.DataFrame(result, columns=['hour', 'avg_fare'])
    
    return jsonify({
        'hours': df['hour'].tolist(),
        'fares': df['avg_fare'].round(2).tolist()
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)
