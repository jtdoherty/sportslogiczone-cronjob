import os
import time
import json
import threading
import requests
from datetime import datetime
from flask import Flask, jsonify
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
MONGODB_URI = os.getenv('MONGODB_ATLAS_URI')
RAPID_API_KEY = os.getenv('RAPID_API_KEY')
RAPID_API_HOST = os.getenv('RAPID_API_HOST')

# Initialize Flask app
app = Flask(__name__)

def setup_mongodb_connection():
    """Establish connection to MongoDB Atlas with proper SSL configuration"""
    try:
        # Configure MongoDB client with proper SSL settings
        client = MongoClient(
            MONGODB_URI,
            connectTimeoutMS=30000,
            socketTimeoutMS=None,
            connect=True,
            maxPoolsize=50,
            retryWrites=True,
            serverSelectionTimeoutMS=5000
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
        "x-rapidapi-key": RAPID_API_KEY,
        "x-rapidapi-host": RAPID_API_HOST
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
        for bet in bets_data:
            collection.update_one(
                {'key': bet['key']},
                {'$set': bet},
                upsert=True
            )
        print(f"Successfully updated {len(bets_data)} bets")
    except Exception as e:
        print(f"Database Update Error: {str(e)}")
        raise

def worker():
    """Background worker function"""
    while True:
        print(f"Starting job at {datetime.utcnow()}")
        try:
            collection = setup_mongodb_connection()
            api_data = fetch_rapid_api_data()
            
            if not api_data.get('advantages'):
                print("No advantages data available")
                time.sleep(300)  # Wait 5 minutes before next attempt
                continue
            
            processed_bets = [
                process_advantage_data(advantage)
                for advantage in api_data['advantages']
            ]
            
            update_database(collection, processed_bets)
            print(f"Job completed successfully at {datetime.utcnow()}")
            
        except Exception as e:
            print(f"Job failed: {str(e)}")
        
        time.sleep(60)  # Wait 5 minutes before next execution

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
        # Test database connection
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