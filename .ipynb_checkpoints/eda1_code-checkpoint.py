import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# Database connection
db_url = 'postgresql://pace:01cc3tuqGRAQ@10.2.4.4:5432/hasura-stage'
engine = create_engine(db_url)

# Fetch data
query = """
SELECT 
    ts.instrument_id,
    i.name AS instrument_name,
    COUNT(te.id) AS error_count,
    COUNT(ts.id) AS test_count,
    MAX(ts.timestamp) - MIN(ts.timestamp) AS downtime
FROM test_samples ts
LEFT JOIN test_errors te ON ts.instrument_id = te.instrument_id
JOIN instrument i ON ts.instrument_id = i.id
WHERE ts.timestamp >= NOW() - INTERVAL '300 days'
GROUP BY ts.instrument_id, i.name;
"""
df = pd.read_sql(query, engine)

# Compute Error Rate
df['error_rate'] = df['error_count'] / df['test_count']
df['downtime_days'] = df['downtime'].dt.total_seconds() / 86400  # Convert downtime to days

# Detect Outliers (Using IQR Method)
Q1 = df['error_rate'].quantile(0.25)
Q3 = df['error_rate'].quantile(0.75)
IQR = Q3 - Q1
df['outlier'] = (df['error_rate'] < (Q1 - 1.5 * IQR)) | (df['error_rate'] > (Q3 + 1.5 * IQR))

# Static Plots for Analysis
fig1 = px.scatter(df, x='test_count', y='error_count', color='outlier',
                  hover_data=['instrument_name'], title="Anomaly Detection: Error Rate")

fig2 = px.box(df, y='error_rate', title="Error Rate Distribution")

fig3 = px.bar(df, x='instrument_name', y='downtime_days', title="Downtime by Instrument")

# Show Plots
fig1.show()
fig2.show()
fig3.show()
