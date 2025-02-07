import sqlalchemy
from sqlalchemy import select, distinct, column
import pymysql
pymysql.install_as_MySQLdb()
import streamlit as st
import pandas as pd
import mysql.connector
import hmac
from flashtext import KeywordProcessor
import time
from scrapper_mysql import scraper
from mysql.connector import pooling

# Define the function to run when the button is clicked
def run_long_process():
    time.sleep(5)

st.set_page_config(layout="wide")

print(st.secrets)

@st.cache_resource
def create_connection_pool():
    return pooling.MySQLConnectionPool(
        pool_name="mypool",
        pool_size=5,
        host=st.secrets.connections.mysql.host,
        user=st.secrets.connections.mysql.username,
        password=st.secrets.connections.mysql.password,
        database=st.secrets.connections.mysql.database
    )

# ✅ Updated MySQL Connection using `username`
conn = st.connection(
    "mysql",
    type="sql",
    ttl=3600,
    host=st.secrets.connections.mysql.host,
    username=st.secrets.connections.mysql.username,  
    password=st.secrets.connections.mysql.password,
    database=st.secrets.connections.mysql.database
)

# ✅ Updated Connection Test Function
def test_connection():
    try:
        test_conn = mysql.connector.connect(
            host=st.secrets.connections.mysql.host,
            user=st.secrets.connections.mysql.username,  
            password=st.secrets.connections.mysql.password,
            database=st.secrets.connections.mysql.database
        )
        test_conn.close()
        return True
    except Exception as e:
        st.error(f"Database Connection Failed: {e}")
        return False

if not test_connection():
    st.error("❌ Failed basic connection test. Please check database credentials.")

@st.cache_data
def get_unique_values(column_name):
    try:
        # Validate column name to prevent SQL injection
        if not isinstance(column_name, str) or not column_name.isidentifier():
            raise ValueError("Invalid column name")

        # SQLAlchemy Core expression
        query = (
            select(distinct(column(column_name)))
            .select_from("newtable")
            .order_by(column(column_name))
        )

        st.write(f"Debug: Query = {query}")  # Shows compiled SQL
        result = conn.query(query)  # conn is SQLAlchemy connection
        return result[column_name].tolist() if not result.empty else []

    except Exception as e:
        st.error(f"Error in get_unique_values: {str(e)}")
        return []

@st.cache_data
def search_data(keyword, agency, procurement_method, fiscal_quarter, job_titles, headcount):
    query = "SELECT * FROM newtable WHERE 1=1"
    params = {}

    if keyword:
        query += " AND `Services Descrption` LIKE :service_desc"
        params["service_desc"] = f"%{keyword}%"
    if agency:
        query += " AND Agency = :agency"
        params["agency"] = agency
    if procurement_method:
        query += " AND `Procurement Method` = :procurement_method"
        params["procurement_method"] = procurement_method
    if fiscal_quarter:
        query += " AND `Fiscal Quarter` = :fiscal_quarter"
        params["fiscal_quarter"] = fiscal_quarter
    if job_titles:
        query += " AND `Job Titles` = :job_titles"
        params["job_titles"] = job_titles
    if headcount:
        query += " AND `Head-count` = :headcount"
        params["headcount"] = headcount

    try:
        print("query", query)  # Debugging line
        print("params", params)  # Debugging line

        result = conn.query(query, params=params)  # ✅ Correct way to pass named parameters

        return result if not result.empty else pd.DataFrame()
    except Exception as e:
        st.error(f"Error executing search query: {e}")
        return pd.DataFrame()




def check_password():
    def login_form():
        with st.form("Credentials"):
            st.text_input("Username", key="username")
            st.text_input("Password", type="password", key="password")
            st.form_submit_button("Log in", on_click=password_entered)

    def password_entered():
        if st.session_state["username"] in st.secrets["passwords"] and hmac.compare_digest(
            st.session_state["password"],
            st.secrets.passwords[st.session_state["username"]],
        ):
            st.session_state["password_correct"] = True
            del st.session_state["password"]
            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False

    if st.session_state.get("password_correct", False):
        return True

    login_form()
    if "password_correct" in st.session_state:
        st.error("😕 User not known or password incorrect")
    return False

def reset_all_states():
    session_vars = [
        'search_clicked',
        'results',
        'selected_rows',
        'previous_selection',
        'editable_dataframe',
        'show_results',
        'show_awards',
        'show_matches'
    ]
    
    for var in session_vars:
        if var in st.session_state:
            del st.session_state[var]
    
    st.cache_data.clear()
    
    st.session_state.reset_trigger = True
    st.rerun()

def main():
    if 'reset_trigger' not in st.session_state:
        st.session_state.reset_trigger = False

    if 'search_clicked' not in st.session_state:
        st.session_state.search_clicked = False
    if 'show_results' not in st.session_state:
        st.session_state.show_results = False
    if 'show_awards' not in st.session_state:
        st.session_state.show_awards = False
    if 'show_matches' not in st.session_state:
        st.session_state.show_matches = False
    if 'results' not in st.session_state:
        st.session_state.results = pd.DataFrame()
    if 'selected_rows' not in st.session_state:
        st.session_state.selected_rows = pd.DataFrame()
    if 'previous_selection' not in st.session_state:
        st.session_state.previous_selection = set()

    st.title("NYC Procurement Intelligence")
    st.sidebar.header("Search Filters")

    keyword = st.sidebar.text_input("Keyword Search (Services Description)", key="keyword")
    agency = st.sidebar.selectbox("Agency", [""] + get_unique_values("Agency"), key="agency")
    procurement_method = st.sidebar.selectbox("Procurement Method", [""] + get_unique_values("Procurement Method"), key="procurement_method")
    fiscal_quarter = st.sidebar.selectbox("Fiscal Quarter", [""] + get_unique_values("Fiscal Quarter"), key="fiscal_quarter")
    job_titles = st.sidebar.selectbox("Job Titles", [""] + get_unique_values("Job Titles"), key="job_titles")
    headcount = st.sidebar.selectbox("Head-count", [""] + [str(x) for x in get_unique_values("Head-count")], key="headcount")

    filters_applied = any([keyword, agency, procurement_method, fiscal_quarter, job_titles, headcount])

    if st.sidebar.button("Search"):
        if filters_applied:
            st.session_state.results = search_data(keyword, agency, procurement_method, fiscal_quarter, job_titles, headcount)
            if not st.session_state.results.empty:
                st.write(f"✅ Found {len(st.session_state.results)} results:")
                st.dataframe(st.session_state.results)
            else:
                st.warning("No results found.")
        else:
            st.warning("Please apply at least one filter before searching.")

    if st.sidebar.button("Reset Search"):
        reset_all_states()
        st.rerun()

if __name__ == "__main__":
    if not check_password():
        st.stop()
    main()
