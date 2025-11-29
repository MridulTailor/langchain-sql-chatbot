import sqlite3
import pandas as pd
import numpy as np
from faker import Faker
import os
from datetime import datetime, timedelta

NUM_ASSETS = 100
NUM_EMPLOYEES = 200
CORE_OVERLAP_SIZE = 20
SENSOR_ROWS = 50000
WORK_ORDER_ROWS = 2000
REVENUE_ROWS = 1000

fake = Faker()
np.random.seed(42)

def ensure_directory():
    if not os.path.exists('data'):
        os.makedirs('data')

def generate_master_data():
    print("Generating Master Data...")
    
    assets = pd.DataFrame({
        'asset_id': [f'AST-{i:03d}' for i in range(1, NUM_ASSETS + 1)],
        'name': [f'Machine-{fake.word().capitalize()}-{i}' for i in range(1, NUM_ASSETS + 1)],
        'location': np.random.choice(['Plant-Austin', 'Plant-Berlin', 'Plant-Tokyo'], NUM_ASSETS),
        'department': np.random.choice(['Assembly', 'Welding', 'Painting', 'Packaging'], NUM_ASSETS),
        'criticality': np.random.choice(['High', 'Medium', 'Low'], NUM_ASSETS, p=[0.2, 0.5, 0.3])
    })
    
    employees = pd.DataFrame({
        'employee_id': [f'EMP-{i:03d}' for i in range(1, NUM_EMPLOYEES + 1)],
        'name': [fake.name() for _ in range(NUM_EMPLOYEES)],
        'role': np.random.choice(['Operator', 'Technician', 'Engineer', 'Manager'], NUM_EMPLOYEES, p=[0.4, 0.3, 0.2, 0.1]),
        'clearance_level': np.random.choice(['L1', 'L2', 'L3', 'L4'], NUM_EMPLOYEES, p=[0.4, 0.3, 0.2, 0.1])
    })

    return assets, employees

def get_db_subset(df, subset_size, core_df):
    core_ids = core_df.index.tolist()
    remaining_pool = df.index.difference(core_ids)
    random_count = subset_size - len(core_ids)
    
    if random_count > 0:
        random_selection = np.random.choice(remaining_pool, random_count, replace=False)
        final_indices = np.concatenate([core_ids, random_selection])
    else:
        final_indices = core_ids

    return df.loc[final_indices].copy()

def create_sensor_db(assets, employees, core_assets, core_employees):
    print("Creating DB1: Sensor Data...")
    path = 'data/db1_sensors.db'
    conn = sqlite3.connect(path)
    
    db_assets = get_db_subset(assets, int(NUM_ASSETS * 0.8), core_assets)
    db_assets.to_sql('assets_shared', conn, if_exists='replace', index=False)
    employees.to_sql('employees_shared', conn, if_exists='replace', index=False)

    readings = pd.DataFrame({
        'reading_id': range(1, SENSOR_ROWS + 1),
        'asset_id': np.random.choice(db_assets['asset_id'], SENSOR_ROWS),
        'timestamp': [datetime.now() - timedelta(minutes=x*5) for x in range(SENSOR_ROWS)]
    })
    
    readings['temperature'] = np.random.normal(75, 5, SENSOR_ROWS)
    readings['vibration'] = np.random.exponential(1.5, SENSOR_ROWS)
    
    mask = (readings['vibration'] > 5.0)
    readings.loc[mask, 'temperature'] += 20
    
    readings.to_sql('sensor_readings', conn, if_exists='replace', index=False)
    conn.close()

def create_maintenance_db(assets, employees, core_assets, core_employees):
    print("Creating DB2: Maintenance Data...")
    path = 'data/db2_maintenance.db'
    conn = sqlite3.connect(path)
    
    db_assets = get_db_subset(assets, int(NUM_ASSETS * 0.6), core_assets)
    db_employees = get_db_subset(employees, int(NUM_EMPLOYEES * 0.5), core_employees)
    
    db_assets.to_sql('assets_shared', conn, if_exists='replace', index=False)
    db_employees.to_sql('employees_shared', conn, if_exists='replace', index=False)

    orders = pd.DataFrame({
        'order_id': [f'WO-{1000+i}' for i in range(WORK_ORDER_ROWS)],
        'asset_id': np.random.choice(db_assets['asset_id'], WORK_ORDER_ROWS),
        'employee_id': np.random.choice(db_employees['employee_id'], WORK_ORDER_ROWS),
        'status': np.random.choice(['Open', 'In Progress', 'Closed', 'Blocked'], WORK_ORDER_ROWS, p=[0.1, 0.2, 0.6, 0.1]),
        'priority': np.random.choice(['Critical', 'High', 'Medium', 'Low'], WORK_ORDER_ROWS),
        'cost': np.round(np.random.lognormal(6, 0.5, WORK_ORDER_ROWS), 2),
        'date_logged': [datetime.now() - timedelta(days=np.random.randint(0, 365)) for _ in range(WORK_ORDER_ROWS)]
    })

    orders.to_sql('work_orders', conn, if_exists='replace', index=False)
    conn.close()

def create_revenue_db(assets, employees, core_assets, core_employees):
    print("Creating DB3: Revenue Data...")
    path = 'data/db3_revenue.db'
    conn = sqlite3.connect(path)
    
    db_assets = get_db_subset(assets, int(NUM_ASSETS * 0.5), core_assets)
    db_assets.to_sql('assets_shared', conn, if_exists='replace', index=False)
    
    revenue = []
    quarters = ['2024-Q1', '2024-Q2', '2024-Q3', '2024-Q4']
    
    for _ in range(REVENUE_ROWS):
        asset = np.random.choice(db_assets['asset_id'])
        is_critical = db_assets.loc[db_assets['asset_id'] == asset, 'criticality'].iloc[0] == 'High'
        base_rev = 50000 if is_critical else 10000
        
        revenue.append({
            'revenue_id': fake.uuid4()[:8],
            'asset_id': asset,
            'quarter': np.random.choice(quarters),
            'amount_usd': np.round(np.random.normal(base_rev, 5000), 2),
            'region': np.random.choice(['North America', 'EMEA', 'APAC'])
        })
        
    pd.DataFrame(revenue).to_sql('asset_revenue', conn, if_exists='replace', index=False)
    conn.close()

def test_cross_db_query():
    print("\nValidating cross-database joins...")
    try:
        conn = sqlite3.connect('data/db1_sensors.db')
        conn.execute("ATTACH DATABASE 'data/db2_maintenance.db' AS maintenance")
        conn.execute("ATTACH DATABASE 'data/db3_revenue.db' AS revenue")
        
        query = """
        SELECT 
            s.asset_id,
            AVG(s.vibration) as avg_vibration,
            COUNT(w.order_id) as maintenance_count,
            SUM(r.amount_usd) as total_revenue
        FROM sensor_readings s
        JOIN maintenance.work_orders w ON s.asset_id = w.asset_id
        JOIN revenue.asset_revenue r ON s.asset_id = r.asset_id
        GROUP BY s.asset_id
        ORDER BY avg_vibration DESC
        LIMIT 5;
        """
        
        df = pd.read_sql(query, conn)
        print("Success! Top 5 assets across all databases:")
        print(df)
        conn.close()
    except Exception as e:
        print(f"Validation failed: {e}")

if __name__ == "__main__":
    ensure_directory()
    all_assets, all_employees = generate_master_data()
    core_assets = all_assets.head(CORE_OVERLAP_SIZE)
    core_employees = all_employees.head(CORE_OVERLAP_SIZE)
    create_sensor_db(all_assets, all_employees, core_assets, core_employees)
    create_maintenance_db(all_assets, all_employees, core_assets, core_employees)
    create_revenue_db(all_assets, all_employees, core_assets, core_employees)
    test_cross_db_query()