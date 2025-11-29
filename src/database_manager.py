import os
from sqlalchemy import create_engine, event
from langchain_community.utilities import SQLDatabase

class CustomSQLDatabase(SQLDatabase):
    def __init__(self, *args, custom_table_info_suffix="", **kwargs):
        super().__init__(*args, **kwargs)
        self._custom_table_info_suffix = custom_table_info_suffix
    
    @property
    def table_info(self) -> str:
        base_info = super().table_info
        return base_info + self._custom_table_info_suffix

class DBManager:
    def __init__(self):
        pass

    def get_db_for_session(self, role):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        
        path_sensors = os.path.join(project_root, 'data', 'db1_sensors.db')
        path_maint = os.path.join(project_root, 'data', 'db2_maintenance.db')
        path_revenue = os.path.join(project_root, 'data', 'db3_revenue.db')
        
        engine = create_engine(f"sqlite:///{path_sensors}")        
        @event.listens_for(engine, "connect")
        def connect(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            if role in ["MaintenanceManager", "PlantDirector"]:
                cursor.execute(f"ATTACH DATABASE '{path_maint}' AS maintenance")
            if role in ["RevenueAnalyst", "PlantDirector"]:
                cursor.execute(f"ATTACH DATABASE '{path_revenue}' AS revenue")
            cursor.close()

        custom_table_info = ""
        
        if role in ["MaintenanceManager", "PlantDirector"]:
            custom_table_info += """

CREATE TABLE maintenance.work_orders (
    order_id TEXT,
    asset_id TEXT,
    employee_id TEXT,
    status TEXT,
    priority TEXT,
    cost REAL,
    date_logged TIMESTAMP
)

/*
2 rows from maintenance.work_orders table:
order_id        asset_id        employee_id     status          priority        cost            date_logged
WO-1000         AST-051         EMP-115         Closed          Low             976.66          2025-05-10 03:37:14.300091
WO-1001         AST-002         EMP-150         Closed          Medium          429.06          2024-12-24 03:37:14.300106
*/
"""
        
        if role in ["RevenueAnalyst", "PlantDirector"]:
            custom_table_info += """

CREATE TABLE revenue.asset_revenue (
    revenue_id TEXT,
    asset_id TEXT,
    quarter TEXT,
    amount_usd REAL,
    region TEXT
)

/*
2 rows from revenue.asset_revenue table:
revenue_id      asset_id        quarter         amount_usd      region
4d29f4ea        AST-006         2024-Q1         6316.72         North America
c332942e        AST-019         2024-Q1         9836.26         APAC
*/
"""
        
        # Create custom SQLDatabase with attached table info
        return CustomSQLDatabase(
            engine=engine,
            sample_rows_in_table_info=2,
            custom_table_info_suffix=custom_table_info
        )
