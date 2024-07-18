import streamlit as st
import configparser
import psycopg2
from psycopg2 import OperationalError, Error
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq

# Function to read configuration from db_config.ini file
def read_config(filename='db_config.ini'):
    config = configparser.ConfigParser()
    try:
        config.read(filename)
        db_config = config['database']
        api_config = config['api']
        return db_config, api_config
    except (configparser.Error, KeyError) as e:
        st.error(f"Error reading configuration file: {e}")
        return None, None

# Function to establish connection with PostgreSQL database using psycopg2
def connect_to_db(config):
    try:
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )
        return conn
    except OperationalError as e:
        st.error(f"Error connecting to the database: {e}")
        return None

# Function to fetch table and column information from the database schema
def get_db_schema(conn, table_name):
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='{table_name}'
        """)
        schema_info = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return schema_info
    except Error as e:
        st.error(f"Error fetching schema information: {e}")
        return []

# Function to create SQL query generation chain
def create_sql_chain(conn, target_table, question, api_key):
    schema_info = get_db_schema(conn, target_table)
    schema_info_str = ', '.join(schema_info)

    template = f"""
        You are an AI assistant tasked with generating SQL queries for a PostgreSQL database. The target table is '{target_table}', and its schema is as follows:
        {schema_info_str}

        Generate a valid SQL query to answer the following question:
        Question: {question}

        Ensure the query adheres to the following requirements:
        - Use exact column names as provided in the schema.
        - Follow PostgreSQL syntax.
        - Use appropriate PostgreSQL functions such as stddev(), stddev_pop(), corr(), etc.
        - If aggregate functions are required, include subqueries or HAVING clauses as necessary.
        - Use the "source" column to differentiate between train (source=0) and test (source=1) data.
        - Utilize window functions like ROW_NUMBER(), RANK(), and LEAD() if needed for advanced analytics.
        - Ensure proper use of GROUP BY and ORDER BY clauses.
        - Use WHERE clauses to filter the data accurately.

        Provide only the SQL query without any additional text.

        SQL Query:
    """
    prompt = ChatPromptTemplate.from_template(template=template)
    llm = ChatGroq(model="llama3-8b-8192", temperature=0.2, groq_api_key=api_key)

    return (
        RunnablePassthrough(assignments={"schema": schema_info, "question": question})
        | prompt
        | llm
        | StrOutputParser()
    )


# Function to execute SQL query on the database and fetch results
def execute_sql_query(conn, sql_query):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        cursor.close()
        return results
    except Error as e:
        st.error(f"Error executing SQL query: {e}")
        return None

# Function to create natural language response based on SQL query results
def create_nlp_answer(conn, sql_query, results, question, api_key):
    if not results:
        results_str = "No results found. This might indicate that there are no matching records or no missing values."
    else:
        results_str = "\n".join([str(row) for row in results])

    template = f"""
        Based on the results of the SQL query '{sql_query}', the answer to your question '{question}' is:

        Query Results:
        {results_str}
    """
    prompt = ChatPromptTemplate.from_template(template=template)
    llm = ChatGroq(model="llama3-8b-8192", temperature=0.2, groq_api_key=api_key)

    return (
        RunnablePassthrough(assignments={"sql_query": sql_query, "results": results_str, "question": question})
        | prompt
        | llm
        | StrOutputParser()
    )

# Main function to run the Streamlit application
def main():
    # Read configuration
    db_config, api_config = read_config('db_config.ini')
    if db_config is None or api_config is None:
        return

    # Connect to database
    conn = connect_to_db(db_config)
    if conn is None:
        return

    target_table = 'turbofan_engine_data_with_rul'

    # Fetch column names to display in the sidebar
    columns = get_db_schema(conn, target_table)

    # Main application layout
    st.set_page_config(page_title="Database Chatbot", page_icon="🤖", layout="wide")
    st.title("Database Chatbot")

    # Sidebar content
    st.sidebar.header("About")
    st.sidebar.write("""
        This application allows you to interact with the `turbofan_engine_data_with_rul` database using natural language queries.
    """)
    st.sidebar.header("Available columns for queries:")
    if columns:
        st.sidebar.write(", ".join(columns))
    else:
        st.sidebar.write("Unable to fetch column names.")

    # User input and query execution
    user_query = st.text_input(f"Ask a question about {target_table}:")
    if st.button("Submit"):
        try:
            with st.spinner('Executing query...'):
                sql_chain = create_sql_chain(conn, target_table, user_query, api_config['groq_api_key'])
                sql_query_response = sql_chain.invoke({})
                sql_query = sql_query_response.strip()

                # Log the generated SQL query for debugging

                results = execute_sql_query(conn, sql_query)
                if results is not None:
                    # Generate natural language response
                    nlp_chain = create_nlp_answer(conn, sql_query, results, user_query, api_config['groq_api_key'])
                    nlp_response = nlp_chain.invoke({})

                    # Display user's question and the natural language response
                    st.write("Answer:")
                    st.write(nlp_response)
                else:
                    st.error("Error retrieving results from the database")
        except Exception as e:
            st.error(f"Error processing your request: {e}")

    if conn:
        conn.close()

if __name__ == "__main__":
    main()
