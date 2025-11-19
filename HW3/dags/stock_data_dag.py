# dags/stock_data_dag.py
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime as dt
from datetime import datetime, timedelta
import os
import json
import pandas as pd
import alpaca_trade_api as tradeapi
from dotenv import load_dotenv

load_dotenv()

# Default args with retries
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Fetch, transform, and save stock data from Alpaca',
    schedule='@once',  
    start_date=dt(2025, 1, 1),
    catchup=False,
)

# INGEST
def fetch_stock_data():
    """Fetch latest bar data from Alpaca"""
    try:
        alpaca_api = tradeapi.REST(
            os.environ['ALPACA_API_KEY'],
            os.environ['ALPACA_SECRET'],
            'https://paper-api.alpaca.markets/v2'
        )
        
        symbol = 'AAPL' 
        bars = alpaca_api.get_bars(symbol, '1Min', limit=1).df

        if bars.empty:
            raise ValueError("No data returned from Alpaca")

        # Move the index (timestamp) into a normal column
        bars = bars.reset_index()  # usually creates a 'timestamp' column

        # Take the first row as a dict
        data = bars.to_dict('records')[0]

        # Add symbol explicitly
        data['symbol'] = symbol

        # Make sure timestamp is a string
        if isinstance(data.get('timestamp'), (datetime, pd.Timestamp)):
            data['timestamp'] = data['timestamp'].isoformat()
        else:
            data['timestamp'] = str(data.get('timestamp'))

        # Save for the next task
        with open('/tmp/raw_stock_data.json', 'w') as f:
            json.dump(data, f)

        
        print(f"✓ Fetched data for {symbol}")
        
    except Exception as e:
        print(f"✗ Error fetching data: {e}")
        raise

# TRANSFORM
def transform_stock_data():
    """Clean and transform the raw data"""
    try:
        with open('/tmp/raw_stock_data.json', 'r') as f:
            data = json.load(f)
        
        # Simple transformations
        transformed = {
            'symbol': data.get('symbol'),
            'timestamp': data.get('timestamp'),
            'open': round(float(data.get('open', 0)), 2),
            'high': round(float(data.get('high', 0)), 2),
            'low': round(float(data.get('low', 0)), 2),
            'close': round(float(data.get('close', 0)), 2),
            'volume': int(data.get('volume', 0)),
            'vwap': round(float(data.get('vwap', 0)), 2),
        }
        
        with open('/tmp/transformed_stock_data.json', 'w') as f:
            json.dump(transformed, f)
        
        print(f"✓ Transformed data for {transformed['symbol']}")
        
    except Exception as e:
        print(f"✗ Error transforming data: {e}")
        raise

# PUBLISH
def publish_stock_data():
    """Save transformed data to CSV"""
    try:
        with open('/tmp/transformed_stock_data.json', 'r') as f:
            data = json.load(f)
        
        df = pd.DataFrame([data])
        output_file = f"/tmp/stock_data_{data['symbol']}.csv"
        
        # Append mode (create if doesn't exist)
        df.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False)
        
        print(f"✓ Published data to {output_file}")
        
    except Exception as e:
        print(f"✗ Error publishing data: {e}")
        raise

# DEFINE TASKS
ingest_task = PythonOperator(
    task_id='fetch_stock_data',
    python_callable=fetch_stock_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_stock_data',
    python_callable=transform_stock_data,
    dag=dag,
)

publish_task = PythonOperator(
    task_id='publish_stock_data',
    python_callable=publish_stock_data,
    dag=dag,
)

# SET DEPENDENCIES
ingest_task >> transform_task >> publish_task