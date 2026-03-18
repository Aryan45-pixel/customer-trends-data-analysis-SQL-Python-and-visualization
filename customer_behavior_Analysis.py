# ==============================
# Customer Shopping Behavior Analysis
# ==============================

import pandas as pd
from sqlalchemy import create_engine

# ------------------------------
# 1. Load Dataset
# ------------------------------
df = pd.read_csv('customer_shopping_behavior.csv')

print(df.head())
print(df.info())

# ------------------------------
# 2. Data Cleaning
# ------------------------------

# Fill missing values in Review Rating
df['Review Rating'] = df.groupby('Category')['Review Rating'].transform(
    lambda x: x.fillna(x.median())
)

# Rename columns
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(' ', '_')
df = df.rename(columns={'purchase_amount_(usd)': 'purchase_amount'})

# ------------------------------
# 3. Feature Engineering
# ------------------------------

# Age group
labels = ['Young Adult', 'Adult', 'Middle-aged', 'Senior']
df['age_group'] = pd.qcut(df['age'], q=4, labels=labels)

# Purchase frequency mapping
frequency_mapping = {
    'Fortnightly': 14,
    'Weekly': 7,
    'Monthly': 30,
    'Quarterly': 90,
    'Bi-Weekly': 14,
    'Annually': 365,
    'Every 3 Months': 90
}

df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping)

# Drop unnecessary column
if 'promo_code_used' in df.columns:
    df = df.drop('promo_code_used', axis=1)

print(df.head())

# ------------------------------
# 4. PostgreSQL Connection
# ------------------------------

username = "postgres"
password = "123456"   # 🔴 CHANGE THIS
host = "localhost"
port = "5432"
database = "customer_behavior"

engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
)

# Upload to PostgreSQL
table_name = "customer"
df.to_sql(table_name, engine, if_exists="replace", index=False)

print("✅ Data successfully stored in PostgreSQL!")
