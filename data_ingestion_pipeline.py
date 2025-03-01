import pandas as pd 
import os 
import json
from db import engine, write_data_to_db

def load_mappings(mapping_file: str) -> dict:
    """Load column mappings from JSON file"""
    with open(mapping_file) as f:
        return json.load(f)

def process_file(file_path: str, mappings: dict) -> pd.DataFrame:
    """Process a single CSV file with appropriate mappings"""
    filename = os.path.basename(file_path)
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    if file_path.endswith('.xlsx'):
        df = pd.read_excel(file_path)

    # Get mappings for this specific file or use default
    file_mappings = mappings.get(filename, mappings.get('default', {}))
    
    # Apply column renaming
    df = df.rename(columns=file_mappings)
    
    # Add missing columns with null values
    required_columns = mappings.get('common_columns', [])
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
    
    # Add supplier_id based on file source
    if "data1" in filename:
        df["supplier_id"] = "1"
    elif "data2" in filename:
        df["supplier_id"] = "2"
    
    return df[required_columns]

def combine_files_with_mapping(data_dir: str, mapping_file: str) -> pd.DataFrame:
    """Process all the files in data_dir with appropriate mappings"""
    combined_data = pd.DataFrame()
    for file in os.listdir(data_dir):
        if (file.endswith('.csv') or file.endswith('.xlsx')) and file in mapping_file:
            file_path = os.path.join(data_dir, file)
            df = process_file(file_path, mapping_file)
            combined_data = pd.concat([combined_data, df], ignore_index=True)
    return combined_data

def main():
    # Configuration
    DATA_DIR = 'data'
    RAW_DATA_DIR = DATA_DIR + '/' + 'raw_data'
    MAPPING_FILE_SUPPLIER = 'mapping/column_mappings_supplier.json'
    MAPPING_FILE_BUYER = 'mapping/column_mappings_buyer.json'

    # Load mapping configuration
    mappings_supplier = load_mappings(os.path.join(DATA_DIR, MAPPING_FILE_SUPPLIER))
    mappings_buyer = load_mappings(os.path.join(DATA_DIR, MAPPING_FILE_BUYER))
    
    # Process all CSV and Excel files in data directory for suppliers
    combined_supplier_data = combine_files_with_mapping(RAW_DATA_DIR, mappings_supplier)
    combined_buyer_data = combine_files_with_mapping(RAW_DATA_DIR, mappings_buyer)
    write_data_to_db(combined_supplier_data, 'suppliers')
    write_data_to_db(combined_buyer_data, 'buyers')    

if __name__ == '__main__':
    main()