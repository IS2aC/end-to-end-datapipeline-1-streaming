from flask import Flask, render_template
from flask_socketio import SocketIO
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time
import threading
import uuid
from datetime import datetime
from eda import KpiOnMyCassandraDataBase as kpi

# Configuration of the Flask application
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key'  # Change this with a secure secret key
socketio = SocketIO(app)

# Configuration of the authentication
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

# Connect to the Cassandra cluster
cluster = Cluster(['localhost'], port=9042, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('spark_streams')

def serialize_row(row):
    """Convert non-serializable values to serializable types."""
    serialized_row = {}
    for key, value in row.items():
        if isinstance(value, (list, dict)):
            serialized_row[key] = value  # Keep lists and dictionaries
        elif isinstance(value, (str, int, float)):
            serialized_row[key] = value  # Base types are already serializable
        elif isinstance(value, uuid.UUID):  # If it's a UUID
            serialized_row[key] = str(value)  # Convert to string
        else:
            serialized_row[key] = str(value)  # Convert any other type to string
    return serialized_row

def fetch_data():
    """Function to continuously fetch data from Cassandra."""
    while True:
        # Get the current date and time
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Query to count rows in the created_users table
        count_query = "SELECT COUNT(*) FROM created_users;"
        count_result = session.execute(count_query)
        row_count = count_result.one()[0]  # Get the count from the result

        # Query to count the number of male
        male_count =  kpi.get_numbers_of_gender(gender = 'male')

        # Query to count the number of female
        female_count =  kpi.get_numbers_of_gender(gender='female')

        # canadian count
        canadian_count =  kpi.get_number_of_nationality(country =  'canada')
        
        # indian count
        indian_count =  kpi.get_number_of_nationality(country =  'india')

        # french count
        french_count =  kpi.get_number_of_nationality(country =  'france')

        # Emit the current time and row count
        socketio.emit('row_count', {'time': current_time, 'count': row_count, 
                                    'male_count': male_count, 'female_count': female_count,
                                    'canadian_count': canadian_count, 'indian_count': indian_count, 
                                    'french_count': french_count})

        time.sleep(3)  # Wait for 3 seconds before checking again

@app.route('/')
def index():
    return render_template('home.html')

if __name__ == '__main__':
    # Start the thread for fetching data
    threading.Thread(target=fetch_data, daemon=True).start()
    socketio.run(app, host='0.0.0.0', port=5000)
