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
    'stock_more',
    default_args=default_args,
    description='Fetch, transform, and save stock data from Alpaca',
    schedule='@once',  # '@daily' '@hourly'
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
        bars = alpaca_api.get_bars(symbol, '1Min', limit=20).df

        if bars.empty:
            raise ValueError("No data returned from Alpaca")

        # Move the index (timestamp) into a normal column
        bars = bars.reset_index()          # now you have 'timestamp'
        bars["symbol"] = symbol            # add symbol column to all rows

        # Convert entire DataFrame to list of dicts
        records = bars.to_dict('records')

        # Make sure timestamps are strings
        for row in records:
            ts = row.get("timestamp")
            if isinstance(ts, (datetime, pd.Timestamp)):
                row["timestamp"] = ts.isoformat()
            else:
                row["timestamp"] = str(ts)

        # Save the list of rows for the next task
        with open('/tmp/raw_stock_data.json', 'w') as f:
            json.dump(records, f)

        print(f"✓ Fetched {len(records)} bars for {symbol}")
        
    except Exception as e:
        print(f"✗ Error fetching data: {e}")
        raise


# TRANSFORM
def transform_stock_data():
    """Clean and transform the raw data"""
    try:
        with open('/tmp/raw_stock_data.json', 'r') as f:
            raw_records = json.load(f)   # list of dicts
        
        transformed_records = []
        for data in raw_records:
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
            transformed_records.append(transformed)
        
        with open('/tmp/transformed_stock_data.json', 'w') as f:
            json.dump(transformed_records, f)
        
        print(f"✓ Transformed {len(transformed_records)} rows")
        
    except Exception as e:
        print(f"✗ Error transforming data: {e}")
        raise


# PUBLISH
def publish_stock_data():
    """Save transformed data to CSV with de-duplication"""
    try:
        with open('/tmp/transformed_stock_data.json', 'r') as f:
            records = json.load(f)
        
        new_df = pd.DataFrame(records)
        if new_df.empty:
            print("No data to publish.")
            return
        
        symbol = new_df["symbol"].iloc[0]
        output_file = f"/tmp/stock_data_{symbol}.csv"

        if os.path.exists(output_file):
            existing_df = pd.read_csv(output_file)
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            # Drop duplicates by symbol + timestamp
            combined = combined.drop_duplicates(subset=["symbol", "timestamp"])
        else:
            combined = new_df

        combined.to_csv(output_file, index=False)
        print(f"✓ Published {len(new_df)} new rows (deduped) to {output_file}")
        
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