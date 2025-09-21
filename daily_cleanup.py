import os
import pyodbc
import logging
import time
from datetime import datetime, timedelta
from email_sender import send_mail  # Import the send_mail function

def get_connection_string() -> str:
    """Generate the database connection string."""
    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.environ['DB_SERVER_NAME']};"
        f"DATABASE={os.environ['DB_NAME']};"
        f"UID={os.environ['DB_USERNAME']};"
        f"PWD={os.environ['DB_PASSWORD']};"
        f"TrustServerCertificate=yes;Encrypt=yes;"
    )

def delete_old_data():
    """Delete old records from the database based on the retention policy."""
    connection_string = get_connection_string()
    today = datetime.now().date()
    retention_date = today - timedelta(days=4)
    
    start_time = time.time()  # Start timing

    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            query = u"DELETE FROM T_DAY_POS_DAILY_SKU_DATA WHERE [当稼動日] < ?"
            logging.info(f"Executing query: {query} with retention_date: {retention_date}")
            cursor.execute(query, retention_date)
            conn.commit()

            rows_deleted = cursor.rowcount
            execution_time = round(time.time() - start_time, 2)  # Calculate execution time in seconds
            
            logging.info(f"Deleted {rows_deleted} records from T_DAY_POS_DAILY_SKU_DATA older than {retention_date} in {execution_time} seconds")

            # Send success email
            
            json_data = {
                "is_file_failed": False,
                "file_name": "N/A",
                "error_message": "N/A",
                "total_time_seconds": execution_time,
                "total_rows_processed": f"Successfully deleted {rows_deleted} records from T_DAY_POS_DAILY_SKU_DATA older than {retention_date}.Daily cleanup done"
            }
            send_mail(json_data, is_file_failed=False)  # ✅ Send success email

    except pyodbc.Error as e:
        execution_time = round(time.time() - start_time, 2)  # Capture execution time on failure
        logging.error(f"Database error during delete_old_data: {e}")

        # Send failure email
        json_data = {
            "is_file_failed": True,
            "file_name": "N/A",
            "error_message": f"Database error: {str(e)}",
            "total_time_seconds": execution_time,
            "total_rows_processed": 0
        }
        send_mail(json_data, is_file_failed=True)  # ✅ Send failure email
        raise
       
    except Exception as e:
        execution_time = round(time.time() - start_time, 2)  # Capture execution time on failure
        logging.error(f"Unexpected error during delete_old_data: {e}")

        # Send failure email
        json_data = {
            "is_file_failed": True,
            "file_name": "N/A",
            "error_message": f"Unexpected error: {str(e)}",
            "total_time_seconds": execution_time,
            "total_rows_processed": 0
        }
        send_mail(json_data, is_file_failed=True)  # ✅ Send failure email
        raise
