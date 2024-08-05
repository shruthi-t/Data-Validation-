import pandas as pd
import psycopg2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection details
DB_NAME = "integer_validation"
DB_USER = "postgres"
DB_PASSWORD = "shruthi@2"
DB_HOST = "localhost"

# Email details
FROM_EMAIL = "sfuture27@outlook.com"
EMAIL_PASSWORD = "jayaram@28"
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587
TO_EMAILS = ["sfuture27@outlook.com"]

# File paths
FILE1_PATH = 'employee.csv' 
FILE2_PATH = 'manufacture.csv'

# Custom flag to control script execution
RUN_SCRIPT_DIRECTLY = True

def create_tables():
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS File1 (
                     name VARCHAR(1000) NOT NULL,
                     phonenumber INT,
                     email VARCHAR(1000),
                     position VARCHAR(1000),
                     salary DECIMAL(10, 2)
                );
                """)
                
                cur.execute("""
                CREATE TABLE IF NOT EXISTS File2 (
                    id SERIAL PRIMARY KEY,
                    productname VARCHAR(5000),
                    productprice INT,    
                    salespersonid INT,
                    legalpersonid INT,
                    developerid INT,
                    testerid INT,
                    hrid INT,
                    FOREIGN KEY (salespersonid) REFERENCES File1(id),
                    FOREIGN KEY (legalpersonid) REFERENCES File1(id),
                    FOREIGN KEY (developerid) REFERENCES File1(id),
                    FOREIGN KEY (testerid) REFERENCES File1(id),
                    FOREIGN KEY (hrid) REFERENCES File1(id)
                );
                """)
        logging.info("Tables created successfully.")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")

def load_data_to_db():
    try:
        if not os.path.exists(FILE1_PATH):
            raise FileNotFoundError(f"{FILE1_PATH} not found.")
        if not os.path.exists(FILE2_PATH):
            raise FileNotFoundError(f"{FILE2_PATH} not found.")

        df_file1 = pd.read_csv(FILE1_PATH)
        df_file2 = pd.read_csv(FILE2_PATH)

        logging.info(f"Loaded {len(df_file1)} rows from {FILE1_PATH}")
        logging.info(f"Loaded {len(df_file2)} rows from {FILE2_PATH}")
        
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                for index, row in df_file1.iterrows():
                    if pd.isna(row['name']) or pd.isna(row['phonenumber']) or pd.isna(row['email']) or pd.isna(row['position']) or pd.isna(row['salary']):
                        continue
                    
                    try:
                        cur.execute(
                            "INSERT INTO File1 (name, phonenumber, email, position, salary) VALUES (%s, %s, %s, %s, %s)",
                            (row['name'], row['phonenumber'], row['email'], row['position'], float(row['salary']))
                        )
                    except Exception as e:
                        logging.error(f"Error inserting row in File1: {row}, error: {e}")

                for index, row in df_file2.iterrows():
                    if pd.isna(row['productname']) or pd.isna(row['productprice']) or pd.isna(row['salespersonid']) or pd.isna(row['legalpersonid']) or pd.isna(row['developerid']) or pd.isna(row['testerid']) or pd.isna(row['hrid']):
                        continue
                    
                    try:
                        cur.execute(
                            "INSERT INTO File2 (productname, productprice, salespersonid, legalpersonid, developerid, testerid, hrid) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (row['productname'], int(row['productprice']), int(row['salespersonid']), int(row['legalpersonid']), int(row['developerid']), int(row['testerid']), int(row['hrid']))
                        )
                    except Exception as e:
                        logging.error(f"Error inserting row in File2: {row}, error: {e}")
                
            conn.commit()
        logging.info("Data loaded into database successfully.")
    except Exception as e:
        logging.error(f"Error loading data to database: {e}")

def validate_data():
    try:
        with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT F2.id, F2.productname, F2.productprice, F2.salespersonid, F2.legalpersonid, F2.developerid, F2.testerid, F2.hrid, F1.salary
                FROM File2 F2
                JOIN File1 F1 ON F1.id IN (F2.salespersonid, F2.legalpersonid, F2.developerid, F2.testerid, F2.hrid)
                WHERE (F2.salespersonid < 0 OR F2.legalpersonid < 0 OR F2.developerid < 0 OR F2.testerid < 0 OR F2.hrid < 0)
                OR (F2.productprice < 0 OR F2.productprice > F1.salary)
                """)

                invalid_rows = cur.fetchall()
                return invalid_rows
    except Exception as e:
        logging.error(f"Error validating data: {e}")
        return []

def send_email(subject, body, to_emails):
    try:
        msg = MIMEMultipart()
        msg['From'] = FROM_EMAIL
        msg['To'] = ", ".join(to_emails)
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(FROM_EMAIL, EMAIL_PASSWORD)
            server.send_message(msg)
        logging.info("Email sent successfully.")
    except Exception as e:
        logging.error(f"Error sending email: {e}")

def main():
    create_tables()
    load_data_to_db()
    invalid_rows = validate_data()
    if invalid_rows:
        logging.info("The following rows failed validation")
        subject = "Data Validation Failed"
        body = "The following rows failed validation:\n" + "\n".join(map(str, invalid_rows))
        send_email(subject, body, TO_EMAILS)
    else:
        logging.info("Data Validation Success")
        subject = "Data Validation Success"
        body = "All data is valid."
        send_email(subject, body, TO_EMAILS)

if RUN_SCRIPT_DIRECTLY:
    main()