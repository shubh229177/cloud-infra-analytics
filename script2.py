import pandas as pd
import sqlite3
import os

def run_etl():
    raw_path = 'data/raw_server_logs.csv'
    db_path = 'data/cloud_infra.db'
    
    if not os.path.exists(raw_path):
        raise FileNotFoundError("Raw log file missing. Run generate_logs.py first.")
        
    print("Starting ETL Process...")
    df = pd.read_csv(raw_path)
    
    # 1. Data Cleaning
    initial_rows = len(df)
    df.drop_duplicates(subset=['log_id'], inplace=True)
    
    # Fill missing CPU values with column median by server_type
    df['cpu_utilization_pct'] = df.groupby('server_type')['cpu_utilization_pct'].transform(
        lambda x: x.fillna(x.median())
    )
    
    # Clip percentage values between 0 and 100
    df['cpu_utilization_pct'] = df['cpu_utilization_pct'].clip(0, 100)
    df['ram_utilization_pct'] = df['ram_utilization_pct'].clip(0, 100)
    
    # 2. Feature Engineering
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['year_month'] = df['timestamp'].dt.strftime('%Y-%m')
    df['hour_of_day'] = df['timestamp'].dt.hour
    
    # Spike Flag (High CPU > 85% AND High RAM > 80%)
    df['is_traffic_spike'] = (df['cpu_utilization_pct'] > 85.0) & (df['ram_utilization_pct'] > 80.0)
    
    # 3. Database Ingestion
    conn = sqlite3.connect(db_path)
    
    # Create fact and dimension tables
    df.to_sql('fact_server_usage', conn, if_exists='replace', index=False)
    
    # Create normalized dimension tables
    teams_df = df[['team']].drop_duplicates().reset_index(drop=True)
    teams_df['team_id'] = range(1, len(teams_df) + 1)
    teams_df.to_sql('dim_teams', conn, if_exists='replace', index=False)
    
    servers_df = df[['server_id', 'server_type', 'region']].drop_duplicates().reset_index(drop=True)
    servers_df.to_sql('dim_servers', conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"ETL Complete! Cleaned {initial_rows} records and loaded into '{db_path}'.")

if __name__ == '__main__':
    run_etl()