import os
import pandas as pd
import sqlite3
import json
import uuid
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("xml_processor.log"),  # Log to a file
        logging.StreamHandler()  # Log to console
    ]
)

class XMLProcessor:
    def __init__(self, directory, db_name, table_name):
        self.directory = directory
        self.db_name = db_name
        self.table_name = table_name
        self.files = []
        self.pipeline_id = str(uuid.uuid4())  # Generate a unique pipeline ID once
        self.load_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Get current datetime once
        self.combined_records = []
        self.combined_err_records = []
        self.eventDf = None
        self.windowed_data = {}

    class RO:
        def __init__(self, order_id: int, status: str,
                    cost: float, technician: str, repair_parts: List[dict]):
            self.order_id = order_id
            self.status = status
            self.cost = cost
            self.technician = technician
            self.repair_parts = repair_parts

        def __repr__(self):
            return (f"RO(order_id={self.order_id}, "
                    f"status={self.status}, cost={self.cost}, technician={self.technician}, "
                    f"repair_parts={self.repair_parts})")
    def process_to_RO(self) -> List[RO]:
        """
        Transforms windowed data into a structured RO format.

        Parameters:
            windowed_data (Dict[str, pd.Series]): The windowed data with timestamps as keys.

        Returns:
            List[RO]: A list of RO objects representing the structured data.
        """
        ro_list = []

        for window, row in self.windowed_data.items():
            # Extract the required fields from the DataFrame row
            ro = self.RO(
                order_id=row['order_id'],
                status=row['status'],
                cost=row['cost'],
                technician=row['technician'],
                repair_parts=json.loads(row['repair_parts'])
            )
            ro_list.append(ro)

        logging.info("Processed data into structured RO format.")
        return ro_list

    def read_directory(self):
        """Reads the directory and lists the XML files."""
        try:
            self.files = [f for f in os.listdir(self.directory) if f.endswith('.xml')]
            return self.files
        except FileNotFoundError:
            logging.error(f"Directory {self.directory} not found.")
            return []

    def flatten_event(self, event, filename):
        """Flattens the XML event structure into a single record, including filename, unique pipeline ID, and load datetime."""
        technician = None
        parts = []

        # Parse the XML string into an ElementTree
        root = ET.fromstring(event)

        # Extract main fields
        order_id = root.find('order_id').text
        date_time = root.find('date_time').text
        status = root.find('status').text
        cost = root.find('cost').text

        # Extract technician
        technician = root.find('repair_details/technician').text

        # Extract repair parts
        repair_parts = root.find('repair_details/repair_parts')
        if repair_parts is not None:
            for part in repair_parts.findall('part'):
                parts.append({
                    'name': part.get('name'),
                    'quantity': part.get('quantity')
                })

        # Create a single record combining all necessary fields
        record = {
            'pipeline_id': self.pipeline_id,
            'order_id': order_id,
            'date_time': date_time,
            'status': status,
            'cost': cost,
            'technician': technician,
            'repair_parts': json.dumps(parts),  
            'filename': filename,
            'load_datetime': self.load_datetime  
        }

        # Log the processing with pipeline information
        logging.info(f"Processed event from file '{filename}'. ")

        return record
    def error_record(self, file, content):
        return {
            'pipeline_id': self.pipeline_id,
            'filename': file,
            'xml_data': json.dumps(content),              
            'load_datetime': self.load_datetime  
        }
    
    def process_xml_files(self):
        """Processes each XML file and returns a combined DataFrame."""
        
        for file in self.files:
            file_path = os.path.join(self.directory, file)
            with open(file_path, 'r') as f:
                    event_data = f.read()
            try:
                # Flatten each event and append to combined_records
                flattened_record = self.flatten_event(event_data, file)  
                self.combined_records.append(flattened_record)              
            except Exception as e:

                logging.error(f"Error processing {file} with pipeline ID: {self.pipeline_id}")
                self.combined_err_records.append(self.error_record(file, event_data))

                
            
        # Convert combined_records to DataFrame
        self.eventDf = pd.DataFrame(self.combined_records)
        error_df = pd.DataFrame(self.combined_err_records)

        return self.eventDf,error_df

    def save_to_sqlite(self, df, table_name, type_of_records):
        """Saves the DataFrame to a SQLite database."""
        try:
            with sqlite3.connect(self.db_name) as conn:
                df.to_sql(table_name, conn, if_exists='append', index=False)
                logging.info(f"{type_of_records} Data saved to table '{table_name}' in database '{self.db_name}' with pipeline ID: {self.pipeline_id}.")
        except Exception as e:
            logging.error(f"Error saving to SQLite: {e}")

    def get_audit_record(self):
        return[ {
            'pipeline_id': self.pipeline_id,
            'no_files_in_dir': len(self.files),
            'no_records_loaded_successfully': len(self.combined_records),
             'no_records_failed_to_load': len(self.combined_err_records),              
            'load_datetime': self.load_datetime  
        }]

    def window_by_datetime(self, window: str) -> Dict[str, pd.DataFrame]:
        """
        Groups the data by a specified time window based on the date_time column
        and retrieves the latest event for each window.

        Parameters:
            data (pd.DataFrame): The input DataFrame containing the data.
            window (str): The time window for grouping (e.g., '1D' for 1 day).

        Returns:
            Dict[str, pd.DataFrame]: A dictionary where keys are window identifiers and 
            values are DataFrames for each window.
        """
        if self.eventDf.empty:
            logging.warning("DataFrame is empty. No windows can be created.")
            return {}
        self.windowed_data = self.eventDf.copy(deep=True)
        # Ensure the date_time column is in datetime format
        self.windowed_data['date_time'] = pd.to_datetime(self.windowed_data['date_time'])
        
        # Set date_time as the index
        self.windowed_data.set_index('date_time', inplace=True)
        
        # Resample the data by the specified window and get the latest event
        resampled = self.windowed_data.resample(window).last()

        # Create a dictionary to hold the results
        self.windowed_data = {}
        
        # Iterate over the resampled data
        for timestamp, row in resampled.iterrows():
            self.windowed_data[timestamp.strftime('%Y-%m-%d %H:%M:%S')] = row
        logging.info(f"Grouped data by window '{window}' and retrieved latest events.")
        return self.windowed_data

    def load_xml_to_sqlite(self):
        """Loads each processed XML file into the SQLite database."""
        good_df, bad_df = self.process_xml_files()
        table_name = self.table_name
        if not good_df.empty:
            self.save_to_sqlite(good_df, table_name, 'Good')
        if not bad_df.empty:
            self.save_to_sqlite(bad_df, table_name+'_error_records', 'Bad')

        audit_df = pd.DataFrame(self.get_audit_record())
        self.save_to_sqlite(audit_df, table_name+'_audit', 'Audit')

