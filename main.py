import os
import io
import zipfile
import pyodbc
import logging
import json
import time
from typing import List, Optional, Generator
from dataclasses import dataclass
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime, timedelta
from contextlib import contextmanager
from threading import Lock
from email_sender import send_mail


# Global lock to prevent multiple instances of ETLProcessor from running concurrently
etl_lock = Lock()


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

def decode_binary_data(binary_data: bytes) -> str:
    """Decode binary data into a readable string (e.g., UTF-8)."""
    try:
        return binary_data.decode("utf-8")  # Adjust encoding if needed
    except UnicodeDecodeError as e:
        logging.error(f"Failed to decode binary data: {e}")
        raise

@dataclass
class Config:
    """Configuration settings for the ETL process."""
    azure_connection_string: str
    source_container: str
    backup_container: str
    checkpoint_container: str  # Name of the checkpoint container
    sql_server: str
    sql_database: str
    sql_username: str
    sql_password: str
    table_name: str  # Raw table: Raw_Files_Data_Daily
    temp_table_name: str  # Temp table: T_DAY_POS_DAILY_SKU_DATA_TEMP
    batch_size: int = 150  # Optimized batch size for efficient insertion
    max_workers: int = 4    # Adjust based on system capabilities
    file_prefix: str = "R520."  # Prefix for daily files
    chunk_size: int = 520        # Size of each data chunk in characters
    combined_chunk_multiplier: int = 1000  # Combine 1000 chunks into one row
    checkpoint_blob_name: str = "checkpoint.json"  # Name of the checkpoint blob

    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables."""
        required_vars = [
            "CONNECTION_STR",
            "SOURCE_CONTAINER_NAME",
            "BACKUP_CONTAINER_NAME",
            "CHECKPOINT_CONTAINER_NAME",  # New required environment variable
            "DB_SERVER_NAME",
            "DB_NAME",
            "DB_USERNAME",
            "DB_PASSWORD",
            "TABLE_NAME",
            "TEMP_TABLE_NAME"
        ]
        missing_vars = [var for var in required_vars if var not in os.environ]
        if missing_vars:
            missing = ", ".join(missing_vars)
            raise EnvironmentError(f"Missing required environment variables: {missing}")

        return cls(
            azure_connection_string=os.environ["CONNECTION_STR"],
            source_container=os.environ["SOURCE_CONTAINER_NAME"],
            backup_container=os.environ["BACKUP_CONTAINER_NAME"],
            checkpoint_container=os.environ["CHECKPOINT_CONTAINER_NAME"],
            sql_server=os.environ["DB_SERVER_NAME"],
            sql_database=os.environ["DB_NAME"],
            sql_username=os.environ["DB_USERNAME"],
            sql_password=os.environ["DB_PASSWORD"],
            table_name=os.environ["TABLE_NAME"],
            temp_table_name=os.environ["TEMP_TABLE_NAME"],
            checkpoint_blob_name=os.environ.get("CHECKPOINT_BLOB_NAME", "checkpoint.json")
        )

class CheckpointManager:
    """Manages checkpointing for the ETL process using Azure Blob Storage."""
    
    def __init__(self, config: Config):
        self.config = config
        self.blob_service_client = BlobServiceClient.from_connection_string(config.azure_connection_string)
        self.checkpoint_container_client = self.blob_service_client.get_container_client(config.checkpoint_container)
        
        # Create the container if it doesn't exist
        try:
            self.checkpoint_container_client.create_container()
            logging.info(f"Created checkpoint container '{config.checkpoint_container}'.")
        except Exception as e:
            logging.info(f"Checkpoint container '{config.checkpoint_container}' already exists or failed to create: {e}")
        
        self.checkpoint_blob_client = self.checkpoint_container_client.get_blob_client(config.checkpoint_blob_name)
        self.lock = Lock()
        self.checkpoint_data = self.load_checkpoint()
    
    def load_checkpoint(self) -> dict:
        """Load checkpoint data from the checkpoint blob."""
        try:
            if self.checkpoint_blob_client.exists():
                download_stream = self.checkpoint_blob_client.download_blob()
                checkpoint_json = download_stream.readall().decode('utf-8')
                data = json.loads(checkpoint_json)
                logging.info(f"Loaded checkpoint from blob '{self.config.checkpoint_blob_name}': {data}")
                return data
            else:
                logging.info(f"Checkpoint blob '{self.config.checkpoint_blob_name}' does not exist. Starting fresh.")
                return {}
        except Exception as e:
            logging.warning(f"Failed to load checkpoint from blob '{self.config.checkpoint_blob_name}': {e}")
            return {}
    
    def save_checkpoint(self, key: str, value):
        """Save a checkpoint value identified by a key to the checkpoint blob."""
        with self.lock:
            self.checkpoint_data[key] = value
            checkpoint_json = json.dumps(self.checkpoint_data)
            try:
                # Delete the existing checkpoint blob if it exists
                if self.checkpoint_blob_client.exists():
                    self.checkpoint_blob_client.delete_blob()
                    logging.info(f"Deleted existing checkpoint blob '{self.config.checkpoint_blob_name}'.")
                
                # Upload the new checkpoint data to the blob
                self.checkpoint_blob_client.upload_blob(checkpoint_json, overwrite=True)
                logging.info(f"Saved checkpoint '{key}': {value} to blob '{self.config.checkpoint_blob_name}'.")
            except Exception as e:
                logging.error(f"Failed to save checkpoint to blob '{self.config.checkpoint_blob_name}': {e}")
                # Depending on requirements, you might want to raise the exception to halt the ETL process
                raise
    
    def get_checkpoint(self, key: str, default=None):
        """Retrieve a checkpoint value by key."""
        return self.checkpoint_data.get(key, default)

class DatabaseConnection:
    """Manages database connections and operations."""
    
    
    def __init__(self, config: Config):
        self.config = config
        self.connection_string = get_connection_string()
        

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = pyodbc.connect(self.connection_string)
        start_time = datetime.utcnow().timestamp()
        try:
            yield conn
        except Exception as e:
            execution_time = round(time.time() - start_time, 2)
            results = {
                "is_file_failed": True,
                 "file_name": "N/A",
                "error_message": f"Stored procedure execution failed: {e}",
                "total_time_seconds": execution_time
            }
            send_mail(results, is_file_failed=True)  # Send failure email
            return
        finally:
            conn.close()

    def execute_stored_procedures(self, procedures: List[str], timeout: Optional[int] = None):
        """
        Execute a list of stored procedures in order.
        
        Args:
            procedures (List[str]): A list of stored procedure names to execute.
            timeout (Optional[int]): Not used in this synchronous implementation.
        """
        start_time = datetime.utcnow()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for proc in procedures:
                try:
                    
                    logging.info(f"Executing stored procedure: {proc} at {start_time.isoformat()}Z")
                    cursor.execute(f"EXEC {proc}")
                    conn.commit()
                    end_time = datetime.utcnow()
                    duration = (end_time - start_time).total_seconds()
                    logging.info(f"Successfully executed: {proc} at {end_time.isoformat()}Z (Duration: {duration} seconds)")
                except pyodbc.Error as e:
                    logging.error(f"Error executing stored procedure '{proc}': {e}")
                    execution_time = round(time.time() - start_time, 2)
                    results = {
                        "is_file_failed": True,
                         "file_name": "N/A",
                        "error_message": f"Stored procedure execution failed: '{proc}': {e}",
                        "total_time_seconds": execution_time
                    }
                    send_mail(results, is_file_failed=True)  # Send failure email
                    return

    def batch_insert(self, batch_data: List[str], max_retries: int = 3):
        """Insert a batch of combined chunk data into the database with retry logic."""
        start_time = datetime.utcnow()
        if not batch_data:
            logging.info("No batch data provided for insertion.")
            return

        retries = 0
        while retries < max_retries:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    query = f"INSERT INTO {self.config.table_name} (raw_file) VALUES (?)"
                    cursor.fast_executemany = True
                    formatted_data = [(data,) for data in batch_data]
                    cursor.executemany(query, formatted_data)
                    conn.commit()
                    logging.info(f"Successfully inserted {len(batch_data)} records into '{self.config.table_name}'.")
                    return
            except pyodbc.Error as e:
                retries += 1
                logging.warning(f"Retry {retries}/{max_retries} for batch insert failed: {e}")
                
                if retries == max_retries:
                    logging.error("Max retries reached. Batch insert failed.")
                    execution_time = round(time.time() - start_time, 2)
                    
                    # Send email notification on final failure
                    results = {
                        "is_file_failed": True,
                        "file_name": "N/A",
                        "error_message": str(e),
                        "total_time_seconds": execution_time
                    }
                    send_mail(results, is_file_failed=True)  # Send failure email
                    raise  # Re-raise the error to let the calling function handle it

                time.sleep(2 ** retries)  # Exponential backoff for retry
            except Exception as e:
                logging.exception(f"Unexpected error during batch insert: {e}")
                execution_time = round(time.time() - start_time.timestamp(), 2)
                # Send email notification on any unexpected error
                results = {
                    "is_file_failed": True,
                    "file_name": "N/A",
                    "error_message": str(e),
                    "total_time_seconds": execution_time
                }
                send_mail(results, is_file_failed=True)  # Send failure email
                raise

class BlobStorageManager:
    """Manages Azure Blob Storage operations."""

    def __init__(self, config: Config):
        self.config = config
        self.service_client = BlobServiceClient.from_connection_string(config.azure_connection_string)
        self.source_container = self.service_client.get_container_client(config.source_container)
        self.backup_container = self.service_client.get_container_client(config.backup_container)

    def get_daily_file(self, target_date: datetime.date) -> Optional[str]:
        """
        Get the single daily file based on the target_date.
        Expected file format: R520.YYYYMMDD_HHMMSS.YYYYMMDDHHMMSS.zip
        """
        prefix = f"POS/daily/data/{self.config.file_prefix}{target_date.strftime('%Y%m%d')}"
        blobs = list(self.source_container.list_blobs(name_starts_with=prefix))

        if not blobs:
            logging.warning(f"No files found for date {target_date.strftime('%Y-%m-%d')}")
            return None
        elif len(blobs) > 1:
            logging.warning(f"Multiple files found for date {target_date.strftime('%Y-%m-%d')}. Processing the first one.")

        return blobs[0].name

    def process_file_in_chunks(
        self,
        file_path: str,
        checkpoint_manager: CheckpointManager,
        chunk_size_bytes: int = 100 * 1024 * 1024  # 100 MB
    ) -> Generator[str, None, None]:
        """
        Process the daily .zip file from blob storage and yield decoded data in 100 MB chunks.
        
        Args:
            file_path (str): Path to the zip file in blob storage.
            checkpoint_manager (CheckpointManager): Instance to manage checkpoints.
            chunk_size_bytes (int): Size of each chunk in bytes (default 100 MB).
        
        Yields:
            str: Decoded data chunks.
        """
        try:
            blob_client = self.source_container.get_blob_client(file_path)
            
            # Stream the blob download to manage memory efficiently
            download_stream = blob_client.download_blob()
            # Read the entire blob into memory (adjust if blobs are extremely large)
            compressed_stream = io.BytesIO()
            download_stream.readinto(compressed_stream)
            compressed_stream.seek(0)

            with zipfile.ZipFile(compressed_stream) as zip_ref:
                zip_contents = zip_ref.namelist()
                if not zip_contents:
                    logging.error(f"No files found inside the zip archive: {file_path}")
                    return

                # Process the first file in the ZIP archive
                with zip_ref.open(zip_contents[0]) as extracted_file:
                    # Retrieve last checkpoint
                    last_position = checkpoint_manager.get_checkpoint('file_position', 0)
                    extracted_file.seek(last_position)
                    
                    while True:
                        # Read a chunk of data
                        data = extracted_file.read(chunk_size_bytes)
                        if not data:
                            break
                        try:
                            decoded_data = decode_binary_data(data)
                            yield decoded_data
                            
                            # Update checkpoint
                            current_position = extracted_file.tell()
                            checkpoint_manager.save_checkpoint('file_position', current_position)
                        except UnicodeDecodeError as e:
                            logging.error(f"Failed to decode binary data at position {extracted_file.tell()}: {e}")
                            raise

            logging.info(f"Completed processing file: {file_path}")

        except zipfile.BadZipFile:
            logging.error(f"Corrupted zip file: {file_path}")
            return
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
            return

    def backup_file(self, file_path: str) -> None:
        """Backup and move the file to the backup container organized by YYYY/YYYYMMDD."""
        try:
            file_name = os.path.basename(file_path)

            # Extract date from index 5 to 12 (YYYYMMDD)
            try:
                date_str = file_name[5:13]  # Extract YYYYMMDD from fixed position
                file_date = datetime.strptime(date_str, "%Y%m%d")
            except (IndexError, ValueError) as e:
                logging.error(f"Failed to extract date from filename {file_name}: {e}")
                return

            year_folder = file_date.strftime("%Y")
            date_folder = file_date.strftime("%Y%m%d")
            backup_path = f"Daily/{year_folder}/{date_folder}/{file_name}"

            source_blob = self.source_container.get_blob_client(file_path)
            backup_blob = self.backup_container.get_blob_client(backup_path)

            logging.info(f"Preparing to backup file. Source Path: {file_path}, Backup Path: {backup_path}")

            if not backup_blob.exists():
                logging.info(f"Starting backup for file: {file_path}")
                copy = backup_blob.start_copy_from_url(source_blob.url)

                # Wait for the copy operation to complete
                while True:
                    properties = backup_blob.get_blob_properties()
                    copy_status = properties.copy.status
                    if copy_status == "pending":
                        logging.info(f"Copying {file_name} to backup is still in progress...")
                        time.sleep(1)
                    elif copy_status == "success":
                        break
                    else:
                        logging.error(f"Failed to copy {file_name} to backup.")
                        raise Exception(f"Failed to copy {file_name} to backup.")

                # Delete the source blob after successful copy
                source_blob.delete_blob()
                logging.info(f"File {file_path} backed up successfully to {backup_path}.")
            else:
                logging.info(f"File {file_path} already exists in backup at {backup_path}.")
        except Exception as e:
            logging.error(f"Error backing up file {file_path}: {e}")

class ETLProcessor:
    """Main ETL process coordinator."""

    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseConnection(config)
        self.blob_manager = BlobStorageManager(config)
        self.checkpoint_manager = CheckpointManager(config)

    def generate_chunks(self, decoded_data: str) -> Generator[str, None, None]:
        """Generator to yield 520-character chunks."""
        for i in range(0, len(decoded_data), self.config.chunk_size):
            yield decoded_data[i:i + self.config.chunk_size]

    def generate_combined_chunks(self, chunks: List[str]) -> Generator[str, None, None]:
        """Generator to yield combined chunks of 1000 chunks each."""
        combined = []
        for chunk in chunks:
            combined.append(chunk)
            if len(combined) == self.config.combined_chunk_multiplier:
                yield ''.join(combined)
                combined = []
        if combined:
            yield ''.join(combined)

    def process(self):
        """Execute the full ETL process."""
        start_time = time.time()  # Fix execution_time error
        total_data_processed_bytes = 0  # Initialize to avoid reference errors
        total_data_processed_mb = 0.0
        total_inserted_rows = 0
        daily_file = "N/A"  # Initialize in case of errors

        with etl_lock:
            run_id = datetime.utcnow().strftime('%Y%m%d%H%M%S')
            logging.info(f"===== ETL Process Started ===== Run ID: {run_id}")
            start_time = time.time()

            try:
                # Reset the checkpoint before starting a new ETL process
                logging.info("Resetting checkpoint to start a new ETL process.")
                self.checkpoint_manager.save_checkpoint('file_position', 0)

                # Calculate target date
                target_date = (datetime.utcnow() + timedelta(hours=5, minutes=30)).date()

                logging.info(f"Target date for processing: {target_date}")

                # Get daily file
                daily_file = self.blob_manager.get_daily_file(target_date)
                if not daily_file:
                    error_msg = f"File not found for date: {target_date.strftime('%Y-%m-%d')}"
                    logging.error(error_msg)
                    execution_time = round(time.time() - start_time, 2)
                    send_mail({
                        "is_file_failed": False,
                        "file_name":"N/A",
                        "error_message": error_msg,
                        "total_time_seconds": execution_time
                    }, is_file_failed=True)
                    return  # Exit the ETL process gracefully

                # Process file in chunks with checkpointing
                decoded_data_generator = self.blob_manager.process_file_in_chunks(daily_file, self.checkpoint_manager)

                # Initialize tracking variables
                total_combined = 0
                total_batches = 0
                total_inserted_rows = 0
                total_data_processed_bytes = 0

                # Initialize batch list and leftover buffer
                current_batch = []
                leftover = ''

                for decoded_data_chunk in decoded_data_generator:
                    # Prepend leftover data from the previous chunk
                    data = leftover + decoded_data_chunk

                    # Calculate the number of complete 520-character chunks
                    num_complete_chunks = len(data) // (self.config.chunk_size * 1)  # 520 characters per chunk
                    complete_data_length = num_complete_chunks * self.config.chunk_size
                    chunks = [data[i:i + self.config.chunk_size] for i in range(0, complete_data_length, self.config.chunk_size)]

                    # Store any leftover data for the next iteration
                    leftover = data[complete_data_length:]

                    # Generate combined chunks
                    combined_chunk_generator = self.generate_combined_chunks(chunks)

                    for combined_chunk in combined_chunk_generator:
                        current_batch.append(combined_chunk)
                        total_combined += 1
                        total_data_processed_bytes += len(combined_chunk.encode('utf-8'))

                        if len(current_batch) == self.config.batch_size:
                            total_batches += 1
                            logging.info(f"===== Processing Batch {total_batches} Started =====")
                            logging.info(f"Processing batch {total_batches} with {len(current_batch)} records.")

                            # Insert the batch
                            self.db.batch_insert(current_batch)

                            # Update and log the total inserted rows
                            inserted_rows = len(current_batch)
                            total_inserted_rows += inserted_rows
                            logging.info(f"Total rows inserted so far: {total_inserted_rows}")

                            # Execute stored procedures after each batch
                            batch_procedures = [
                                "SP_Process_Daily_SKU_Data_Temp",
                                "SP_T_DAY_TEN_KYAKUSU_DATA",
                                "SP_Process_Daily_SKU_Data"
                            ]
                            self.db.execute_stored_procedures(batch_procedures)

                            logging.info(f"===== Processing Batch {total_batches} Completed =====")

                            # Reset current batch
                            current_batch = []

                # Handle any remaining chunks and leftover data
                if leftover:
                    logging.info("Handling leftover data after processing all chunks.")
                    # If leftover data is substantial, you may decide to process it
                    # For this example, we'll attempt to create a final chunk if possible
                    if len(leftover) >= self.config.chunk_size:
                        chunks = [leftover[i:i + self.config.chunk_size] for i in range(0, len(leftover), self.config.chunk_size)]
                        combined_chunk_generator = self.generate_combined_chunks(chunks)
                        for combined_chunk in combined_chunk_generator:
                            current_batch.append(combined_chunk)
                            total_combined += 1
                            total_data_processed_bytes += len(combined_chunk.encode('utf-8'))

                            if len(current_batch) == self.config.batch_size:
                                total_batches += 1
                                logging.info(f"===== Processing Batch {total_batches} Started =====")
                                logging.info(f"Processing batch {total_batches} with {len(current_batch)} records.")

                                # Insert the batch
                                self.db.batch_insert(current_batch)

                                # Update and log the total inserted rows
                                inserted_rows = len(current_batch)
                                total_inserted_rows += inserted_rows
                                logging.info(f"Total rows inserted so far: {total_inserted_rows}")

                                # Execute stored procedures after each batch
                                batch_procedures = [
                                    "SP_Process_Daily_SKU_Data_Temp",
                                    "SP_T_DAY_TEN_KYAKUSU_DATA",
                                    "SP_Process_Daily_SKU_Data"
                                ]
                                self.db.execute_stored_procedures(batch_procedures)

                                logging.info(f"===== Processing Batch {total_batches} Completed =====")

                                # Reset current batch
                                current_batch = []

                    # Yield any remaining leftover data as a final chunk
                    if leftover:
                        logging.info(f"Yielding final leftover data of length {len(leftover)} characters.")
                        current_batch.append(leftover)
                        total_combined += 1
                        total_data_processed_bytes += len(leftover.encode('utf-8'))

                # Insert any remaining combined chunks
                if current_batch:
                    total_batches += 1
                    logging.info(f"===== Processing Final Batch {total_batches} Started =====")
                    logging.info(f"Processing final batch {total_batches} with {len(current_batch)} records.")

                    self.db.batch_insert(current_batch)

                    # Update and log the total inserted rows
                    inserted_rows = len(current_batch)
                    total_inserted_rows += inserted_rows
                    logging.info(f"Total rows inserted so far: {total_inserted_rows}")

                    # Execute stored procedures after the final batch
                    batch_procedures = [
                        "SP_Process_Daily_SKU_Data_Temp",
                        "SP_T_DAY_TEN_KYAKUSU_DATA",
                        "SP_Process_Daily_SKU_Data"
                    ]
                    self.db.execute_stored_procedures(batch_procedures)

                    logging.info(f"===== Processing Final Batch {total_batches} Completed =====")

                # Execute final stored procedures once after all batches are inserted
                final_procedures = [
                    "SP_Process_Daily_Sales_Data",
                    "SP_T_DAY_TEN_SALES_FRONT_DATA_DAILY"
                ]
                logging.info("===== Executing Final Stored Procedures =====")
                self.db.execute_stored_procedures(final_procedures)
                logging.info("===== Final Stored Procedures Executed Successfully =====")

                # Backup the processed file
                self.blob_manager.backup_file(daily_file)

                # Log the total data processed and total rows inserted
                total_data_processed_mb = round(total_data_processed_bytes / (1024 * 1024), 2)
                logging.info(f"Total data processed: {total_data_processed_mb:.2f} MB ({total_data_processed_bytes} bytes)")
                logging.info(f"Total rows inserted into '{self.config.table_name}': {total_inserted_rows}")
                logging.info(f"Total number of batches processed: {total_batches}")

                logging.info(f"===== ETL Process Completed Successfully ===== Run ID: {run_id}")

                # Reset checkpoint after successful completion
                execution_time = round(time.time() - start_time, 2)
                results = {
                "is_file_failed": False,
                "error_message": "None",
                "file_name": os.path.basename(daily_file),  # Extract only the file name
                "total_time_seconds": execution_time,
                "total_rows_processed": str(total_inserted_rows),
                "total_data_processed_mb": total_data_processed_mb
            }
                send_mail(results, is_file_failed=False)
                self.checkpoint_manager.save_checkpoint('file_position', 0)
                return

            except Exception as e:
                logging.exception(f"ETL process failed: {e} Run ID: {run_id}")
                execution_time = round(time.time() - start_time, 2)
                results = {
                    "is_file_failed": True,
                    "file_name": daily_file,
                    "error_message": str(e),
                    "total_time_seconds": execution_time,
                    "total_rows_processed": str(total_inserted_rows),
                    "total_data_processed_mb": round(total_data_processed_mb,2)
                }
                send_mail(results, is_file_failed=True)  # Send failure email in case of error
                return

def main():
    """Main entry point for the ETL process."""
    start_time = time.time()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    try:
        config = Config.from_env()
        processor = ETLProcessor(config)
        processor.process()
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        execution_time = round(time.time() - start_time, 2)
        results = {
                    "is_file_failed": True,
                    "error_message": str(e),
                    "total_time_seconds": execution_time
                }
        send_mail(results, is_file_failed=True)  # Send failure email in case of error
        raise
