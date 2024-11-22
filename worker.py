import os
import time
import json
import threading
import certifi
import requests
import dns.resolver
from datetime import datetime
from flask import Flask, jsonify
from pymongo import MongoClient, errors
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration - Use class to prevent variable scope issues
class Config:
    MONGODB_URI = os.getenv('MONGODB_ATLAS_URI')
    RAPID_API_KEY = os.getenv('RAPID_API_KEY')
    RAPID_API_HOST = os.getenv('RAPID_API_HOST')
    
    @classmethod
    def validate(cls):
        if not cls.MONGODB_URI:
            raise ValueError("MONGODB_ATLAS_URI not set in environment")
        if not cls.RAPID_API_KEY:
            raise ValueError("RAPID_API_KEY not set in environment")
        if not cls.RAPID_API_HOST:
            raise ValueError("RAPID_API_HOST not set in environment")

# Initialize Flask app
app = Flask(__name__)

def setup_mongodb_connection():
    """Establish connection to MongoDB Atlas with proper SSL configuration"""
    try:
        # Validate configuration
        Config.validate()
        
        # Configure MongoDB client with proper SSL settings
        client = MongoClient(
            Config.MONGODB_URI,
            tls=True,
            tlsCAFile=certifi.where(),
            connectTimeoutMS=30000,
            socketTimeoutMS=None,
            connect=True,
            maxPoolsize=50,
            retryWrites=True,
            serverSelectionTimeoutMS=30000,
            ssl_cert_reqs='CERT_REQUIRED'
        )
        
        # Force a connection to verify it works
        client.admin.command('ping')
        print("Connected successfully to MongoDB Atlas")
        
        db = client.sports_betting
        return db.bets
    except Exception as e:
        print(f"MongoDB Connection Error: {str(e)}")
        raise

def fetch_rapid_api_data():
    """Fetch data from RapidAPI endpoint"""
    url = "https://sportsbook-api2.p.rapidapi.com/v0/advantages/"
    querystring = {"type": "PLUS_EV_AVERAGE"}
    headers = {
        "x-rapidapi-key": Config.RAPID_API_KEY,
        "x-rapidapi-host": Config.RAPID_API_HOST
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"RapidAPI Request Error: {str(e)}")
        raise

def process_advantage_data(advantage):
    """Process individual advantage data"""
    outcomes = advantage.get('outcomes', [])
    participant = outcomes[0].get('participant') if outcomes else None
    implied_probability = advantage.get('marketStatistics', [{}])[0].get('value')
    outcome_payout = outcomes[0].get('payout') if outcomes else None

    profit_potential = None
    EV = None

    if implied_probability is not None and outcome_payout is not None:
        profit_potential = (outcome_payout - 1) * 100
        implied_probability_decimal = implied_probability / 100
        EV = (implied_probability_decimal * profit_potential) - ((1 - implied_probability_decimal) * 100)

    return {
        'key': advantage['key'],
        'edge': advantage['type'],
        'lastFoundAt': advantage['lastFoundAt'],
        'type': advantage['market']['type'],
        'market_name': advantage['market']['event']['name'],
        'participants': [p['name'] for p in advantage['market']['event']['participants']],
        'outcome_payout': outcome_payout,
        'source': outcomes[0].get('source') if outcomes else None,
        'participant': participant['name'] if participant else None,
        'sport': participant['sport'] if participant else None,
        'implied_probability': implied_probability,
        'profit_potential': profit_potential,
        'EV': EV,
        'event_start_time': advantage['market']['event'].get('startTime'),
        'competition_instance_name': advantage['market']['event'].get('competitionInstance', {}).get('name'),
        'updated_at': datetime.utcnow()
    }

def update_database(collection, bets_data):
    """Update MongoDB with new betting data"""
    try:
        if not bets_data:
            print("No bets data to update")
            return

        operations = []
        for bet in bets_data:
            operations.append(
                {
                    'replaceOne': {
                        'filter': {'key': bet['key']},
                        'replacement': bet,
                        'upsert': True
                    }
                }
            )
        
        result = collection.bulk_write(operations, ordered=False)
        print(f"Successfully processed {len(operations)} bets")
        print(f"Modified: {result.modified_count}, Upserted: {result.upserted_count}")
    except Exception as e:
        print(f"Database Update Error: {str(e)}")
        raise

def worker():
    """Background worker function"""
    retry_count = 0
    max_retries = 3
    
    while True:
        print(f"Starting job at {datetime.utcnow()}")
        try:
            # Initialize MongoDB connection
            collection = setup_mongodb_connection()
            
            # Fetch and process data
            api_data = fetch_rapid_api_data()
            
            if not api_data.get('advantages'):
                print("No advantages data available")
                time.sleep(60)  # Wait 5 minutes
                continue
            
            processed_bets = [
                process_advantage_data(advantage)
                for advantage in api_data['advantages']
            ]
            
            # Update database
            update_database(collection, processed_bets)
            print(f"Job completed successfully at {datetime.utcnow()}")
            retry_count = 0  # Reset retry count on success
            
        except (errors.ConnectionFailure, errors.ServerSelectionTimeoutError) as e:
            retry_count += 1
            print(f"Connection error (attempt {retry_count}/{max_retries}): {str(e)}")
            if retry_count >= max_retries:
                print("Max retries reached, waiting for next cycle")
                retry_count = 0
                time.sleep(60)
            else:
                time.sleep(30)  # Wait 30 seconds before retry
            continue
            
        except Exception as e:
            print(f"Job failed: {str(e)}")
            time.sleep(60)  # Wait 5 minutes
        
        time.sleep(60)  # Regular 5-minute interval

# Start background worker thread
worker_thread = threading.Thread(target=worker, daemon=True)
worker_thread.start()

@app.route('/')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat()
    })

@app.route('/status')
def worker_status():
    """Worker status endpoint"""
    try:
        collection = setup_mongodb_connection()
        last_update = collection.find_one(
            {},
            {'updated_at': 1},
            sort=[('updated_at', -1)]
        )
        
        return jsonify({
            'status': 'healthy',
            'database_connected': True,
            'last_update': last_update['updated_at'] if last_update else None,
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'database_connected': False,
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

if __name__ == '__main__':
    # Get port from environment variable for Render compatibility
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)