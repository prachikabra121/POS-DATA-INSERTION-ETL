import azure.functions as func
import logging
import json
import requests
import re
from datetime import datetime
from typing import Dict
import os
import pytz


# Constants (Replace with your actual values)

LOGIC_APP_URL = os.getenv("LOGIC_APP_URL")
EMAIL_TO = os.getenv("EMAIL_TO")
ENV = "BELC-BI-ESS-BASE-PROD"

SUCCESS_SUBJECT = "🎉 Success! POS Data Inserted Successfully into the Database!"
FAILURE_SUBJECT = "⚠️ Error: Failed to Insert POS Data into the Database."



def send_mail(json_data: Dict, is_file_failed: bool):
    """
    Send an email notification based on pipeline success or failure.

    Args:
        json_data (dict): Dictionary containing pipeline execution details.
    """
    function_name = "BELC-BI-ESS-BASE-Daily-POS-Ingestion-prod"
    is_file_failed = json_data.get("is_file_failed", False)
    file_name = json_data.get("file_name", "N/A")
    error_message = json_data.get("error_message", "None")
    total_time_seconds = float(json_data.get("total_time_seconds", 0))
    total_rows_processed = json_data.get("total_rows_processed", "N/A")
    total_data_processed_mb = float(json_data.get("total_data_processed_mb", 0))


    # Convert total time to minutes
    total_time_minutes = round(total_time_seconds / 60, 2)

    # Convert current time to JST (UTC+9)
    utc_now = datetime.utcnow()
    jst_now = utc_now.replace(tzinfo=pytz.utc).astimezone(pytz.timezone("Asia/Tokyo"))
    execution_time_jst = jst_now.strftime("%Y-%m-%d")

    # Define pipeline status message
    pipeline_status = "✅ Pipeline executed successfully!" if not is_file_failed else f"❌ Pipeline execution failed! Error: {error_message}"

    message = f"""
    <p><b>Environment:</b> {ENV}</p>
    <p><b>Pipeline Status:</b> {pipeline_status}</p>
    <p><b>Function Details:</b></p>
    <ul>
        <li>📁<b>Function Name:</b> {function_name}</li>
        <li>⚠️ <b>Error Message:</b> {error_message}</li>
    </ul>
    <p><b>Execution Summary:</b></p>
    <ul>
        <li>📊 <b>File Name:</b> {file_name}</li>
        <li>📊 <b>Total Rows Processed:</b> {total_rows_processed}</li>
        <li>💾 <b>Total Data Processed:</b> {total_data_processed_mb} MB</li>
        <li>⏳ <b>Total Execution Time:</b> {total_time_minutes} minutes</li>
        <li>🕒 <b>Execution Time (JST):</b> {execution_time_jst}</li>
    </ul>
    <p style="color: gray; font-style: italic; font-size: 12px; border-top: 1px solid #ccc; padding-top: 10px;">
        🚫 <b>This is an automated system message. Please do not reply.</b> 
    </p>
    """
    
    subject = FAILURE_SUBJECT if is_file_failed else SUCCESS_SUBJECT

    email_data = {
        "EmailFrom": "sureshjindam@vebuin.com",
        "EmailTo": EMAIL_TO,
        "Subject": subject,
        "Body": message,
    }

    logging.info(f"Sending email with subject: {subject}")

    try:
        response = requests.post(LOGIC_APP_URL, json=email_data)

        if response.status_code == 200:
            logging.info("Email sent successfully via Logic App.")
        else:
            logging.error(f"Failed to send email. Status: {response.status_code}, Response: {response.text}")

    except requests.RequestException as e:
        logging.error(f"Request error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
