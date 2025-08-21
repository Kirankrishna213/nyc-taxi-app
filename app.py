from flask import Flask, render_template, jsonify
from clickhouse_driver import Client
import pandas as pd
import os
from datetime import datetime, timedelta

app = Flask(__name__)

# ClickHouse connection with environment variables
def get_clickhouse_client():
    return Client(
        host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
        user=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD', ''),
        database=os.getenv('CLICKHOUSE_DATABASE', 'default'),
        secure=os.getenv('CLICKHOUSE_SECURE', 'False').lower() == 'true'
    )

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health')
def health():
    try:
        client = get_clickhouse_client()
        client.execute('SELECT 1')
        return jsonify({'status': 'healthy', 'database': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'database': 'disconnected', 'error': str(e)}), 500

@app.route('/api/trips_by_hour')
def trips_by_hour():
    try:
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
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ... keep the other API routes the same (trips_by_day, top_locations, avg_fare_by_hour)

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=os.getenv('DEBUG', 'False').lower() == 'true')
