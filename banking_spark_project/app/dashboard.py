import streamlit as st
import pandas as pd
import sqlalchemy
import os
import plotly.express as px

DB_URI = os.getenv("DB_URI", "postgresql://airflow:airflow@postgres:5432/airflow")
st.set_page_config(layout="wide")
st.title("🐘 Big Data: Spark + Hadoop")

def load_data(query):
    try:
        engine = sqlalchemy.create_engine(DB_URI)
        return pd.read_sql(query, engine)
    except:
        return pd.DataFrame()

if st.button("Обновить"): st.rerun()

col1, col2 = st.columns(2)
with col1:
    df = load_data("SELECT * FROM region_stats_spark ORDER BY client_count DESC LIMIT 15")
    if not df.empty: st.plotly_chart(px.bar(df, x='adminarea', y='client_count'))
with col2:
    df = load_data("SELECT eng_cat, count(*) as cnt FROM client_predictions_spark GROUP BY eng_cat LIMIT 10")
    if not df.empty: st.plotly_chart(px.pie(df, values='cnt', names='eng_cat'))