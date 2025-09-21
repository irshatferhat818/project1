#!/usr/bin/env python3
"""
Employee ETL Process for SCD Type II Implementation
===================================================

This script demonstrates a complete ETL process for handling employee data updates
in a Slowly Changing Dimension Type II (SCD Type II) table.

Key Features:
- Parses complex nested JSON data with unwanted fields
- Implements SCD Type II logic for employee dimension table
- Handles new employee inserts, address changes, and name changes
- Uses PostgreSQL parameter binding for security
- Comprehensive error handling and logging

Usage:
    python3 python_employee_etl.py employee_20250813.json

Author: Metrica Teaching ETL Demo
Date: 2024-08-13
"""

import json
import sys
import psycopg2
import logging
from collections import OrderedDict
from datetime import datetime, date
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'etl_employee_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EmployeeETL:
    """
    ETL processor for employee data with SCD Type II implementation
    """
    
    def __init__(self, host='localhost', database='metrica', user='postgres', password='postgres'):
        """Initialize database connection parameters"""
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None
        
    def connect_to_database(self) -> bool:
        """
        Establish connection to PostgreSQL database
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            logger.info(f"Successfully connected to database: {self.database}")
            return True
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            return False
            
    def close_connection(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")
        
    def parse_employee_data(self, json_file_path: str) -> OrderedDict:
        """
        Parse the complex nested JSON file and extract relevant employee data
        
        Args:
            json_file_path (str): Path to the JSON file
            
        Returns:
            OrderedDict: Parsed employee data organized by operation type
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                
            logger.info(f"Successfully loaded JSON file: {json_file_path}")
            logger.info(f"Total records in file: {data.get('total_records', 'Unknown')}")
            
            # Initialize ordered dict to maintain processing order
            parsed_data = OrderedDict({
                'new_employees': [],
                'address_updates': [],
                'name_and_address_updates': [],
                'metadata': {
                    'source': data.get('data_source', 'Unknown'),
                    'timestamp': data.get('export_timestamp', 'Unknown'),
                    'total_records': data.get('total_records', 0)
                }
            })
            
            # Process each employee record
            for employee_record in data.get('employees', []):
                operation_type = employee_record.get('operation_type', '')
                employee_data = employee_record.get('employee_data', {})
                
                if operation_type == 'INSERT':
                    # Extract data for new employee insertion
                    parsed_employee = self._extract_new_employee_data(employee_data)
                    parsed_data['new_employees'].append(parsed_employee)
                    
                elif operation_type == 'UPDATE_ADDRESS':
                    # Extract data for address updates
                    parsed_update = self._extract_address_update_data(employee_data)
                    parsed_data['address_updates'].append(parsed_update)
                    
                elif operation_type == 'UPDATE_NAME_ADDRESS':
                    # Extract data for name and address updates
                    parsed_update = self._extract_name_address_update_data(employee_data)
                    parsed_data['name_and_address_updates'].append(parsed_update)
                    
            logger.info(f"Parsed {len(parsed_data['new_employees'])} new employees")
            logger.info(f"Parsed {len(parsed_data['address_updates'])} address updates")
            logger.info(f"Parsed {len(parsed_data['name_and_address_updates'])} name/address updates")
            
            return parsed_data
            
        except FileNotFoundError:
            logger.error(f"JSON file not found: {json_file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format: {e}")
            raise
        except Exception as e:
            logger.error(f"Error parsing JSON data: {e}")
            raise
            
    def _extract_new_employee_data(self, employee_data: Dict) -> Dict:
        """Extract and clean data for new employee insertion"""
        personal_info = employee_data.get('personal_info', {})
        contact_info = employee_data.get('contact_info', {})
        address = contact_info.get('address', {})
        employment_data = employee_data.get('employment_data', {})
        
        return {
            'employee_id': employee_data.get('employee_id'),
            'first_name': personal_info.get('first_name'),
            'last_name': personal_info.get('last_name'),
            'full_name': f"{personal_info.get('first_name', '')} {personal_info.get('last_name', '')}".strip(),
            'email_address': contact_info.get('email_address'),
            'phone_number': contact_info.get('phone_number'),
            'birth_date': personal_info.get('birth_date'),
            'gender': personal_info.get('gender'),
            'ethnicity': personal_info.get('ethnicity'),
            'nationality': personal_info.get('nationality'),
            'marital_status': personal_info.get('marital_status'),
            'address_line1': address.get('address_line1'),
            'address_line2': address.get('address_line2'),
            'city': address.get('city'),
            'state_province': address.get('state_province'),
            'postal_code': address.get('postal_code'),
            'country_code': address.get('country_code'),
            'hire_date': employment_data.get('hire_date'),
            'is_active': True,
            'effective_date': date.today(),
            'expiry_date': date(9999, 12, 31),
            'is_current': True
        }
        
    def _extract_address_update_data(self, employee_data: Dict) -> Dict:
        """Extract and clean data for address updates"""
        contact_info = employee_data.get('contact_info', {})
        address = contact_info.get('address', {})
        
        return {
            'employee_id': employee_data.get('employee_id'),
            'address_line1': address.get('address_line1'),
            'address_line2': address.get('address_line2'),
            'city': address.get('city'),
            'state_province': address.get('state_province'),
            'postal_code': address.get('postal_code'),
            'country_code': address.get('country_code'),
            'effective_date': date.today()
        }
        
    def _extract_name_address_update_data(self, employee_data: Dict) -> Dict:
        """Extract and clean data for name and address updates"""
        personal_info = employee_data.get('personal_info', {})
        contact_info = employee_data.get('contact_info', {})
        address = contact_info.get('address', {})
        
        return {
            'employee_id': employee_data.get('employee_id'),
            'last_name': personal_info.get('last_name'),
            'address_line1': address.get('address_line1'),
            'address_line2': address.get('address_line2'),
            'city': address.get('city'),
            'state_province': address.get('state_province'),
            'postal_code': address.get('postal_code'),
            'country_code': address.get('country_code'),
            'effective_date': date.today()
        }
        
    def set_employee(self, parsed_data: OrderedDict) -> bool:
        """
        Process all employee data using SCD Type II logic
        
        Args:
            parsed_data (OrderedDict): Parsed employee data
            
        Returns:
            bool: True if all operations successful, False otherwise
        """
        try:
            # Start transaction
            self.connection.autocommit = False
            
            # Process new employees
            new_count = self._insert_new_employees(parsed_data['new_employees'])
            
            # Process address updates
            address_count = self._update_employee_addresses(parsed_data['address_updates'])
            
            # Process name and address updates
            name_count = self._update_employee_names_addresses(parsed_data['name_and_address_updates'])
            
            # Commit transaction
            self.connection.commit()
            
            logger.info(f"ETL Process completed successfully:")
            logger.info(f"  - New employees inserted: {new_count}")
            logger.info(f"  - Address updates processed: {address_count}")
            logger.info(f"  - Name/address updates processed: {name_count}")
            
            return True
            
        except Exception as e:
            # Rollback transaction on error
            self.connection.rollback()
            logger.error(f"ETL process failed, transaction rolled back: {e}")
            return False
        finally:
            # Reset autocommit
            self.connection.autocommit = True
            
    def _insert_new_employees(self, new_employees: List[Dict]) -> int:
        """Insert new employees using parameterized queries"""
        insert_sql = """
        INSERT INTO hr.dim_employee (
            employee_id, first_name, last_name, full_name, email_address, phone_number,
            birth_date, gender, ethnicity, nationality, marital_status,
            address_line1, address_line2, city, state_province, postal_code, country_code,
            hire_date, termination_date, is_active, effective_date, expiry_date, is_current,
            created_date, updated_date
        ) VALUES (
            %(employee_id)s, %(first_name)s, %(last_name)s, %(full_name)s, %(email_address)s, %(phone_number)s,
            %(birth_date)s, %(gender)s, %(ethnicity)s, %(nationality)s, %(marital_status)s,
            %(address_line1)s, %(address_line2)s, %(city)s, %(state_province)s, %(postal_code)s, %(country_code)s,
            %(hire_date)s, NULL, %(is_active)s, %(effective_date)s, %(expiry_date)s, %(is_current)s,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
        """
        
        count = 0
        for employee in new_employees:
            try:
                self.cursor.execute(insert_sql, employee)
                count += 1
                logger.info(f"Inserted new employee: {employee['employee_id']} - {employee['full_name']}")
            except psycopg2.Error as e:
                logger.error(f"Failed to insert employee {employee['employee_id']}: {e}")
                raise
                
        return count
        
    def _update_employee_addresses(self, address_updates: List[Dict]) -> int:
        """Update employee addresses using SCD Type II logic"""
        count = 0
        
        for update in address_updates:
            try:
                # Step 1: Close current record
                close_sql = """
                UPDATE hr.dim_employee 
                SET expiry_date = %s - INTERVAL '1 day',
                    is_current = FALSE,
                    updated_date = CURRENT_TIMESTAMP
                WHERE employee_id = %s 
                  AND is_current = TRUE
                """
                
                self.cursor.execute(close_sql, (update['effective_date'], update['employee_id']))
                
                # Step 2: Get existing employee data for the new record
                select_sql = """
                SELECT employee_id, first_name, last_name, full_name, email_address, phone_number,
                       birth_date, gender, ethnicity, nationality, marital_status,
                       hire_date, termination_date, is_active
                FROM hr.dim_employee 
                WHERE employee_id = %s 
                  AND expiry_date = %s - INTERVAL '1 day'
                ORDER BY employee_key DESC 
                LIMIT 1
                """
                
                self.cursor.execute(select_sql, (update['employee_id'], update['effective_date']))
                existing_data = self.cursor.fetchone()
                
                if existing_data:
                    # Step 3: Insert new record with updated address
                    # existing_data order: employee_id, first_name, last_name, full_name, email_address, phone_number,
                    #                      birth_date, gender, ethnicity, nationality, marital_status,
                    #                      hire_date, termination_date, is_active
                    
                    insert_params = [
                        existing_data[0],  # employee_id
                        existing_data[1],  # first_name
                        existing_data[2],  # last_name
                        existing_data[3],  # full_name
                        existing_data[4],  # email_address
                        existing_data[5],  # phone_number
                        existing_data[6],  # birth_date
                        existing_data[7],  # gender
                        existing_data[8],  # ethnicity
                        existing_data[9],  # nationality
                        existing_data[10], # marital_status
                        update['address_line1'],    # new address_line1
                        update['address_line2'],    # new address_line2
                        update['city'],             # new city
                        update['state_province'],   # new state_province
                        update['postal_code'],      # new postal_code
                        update['country_code'],     # new country_code
                        existing_data[11], # hire_date
                        existing_data[12], # termination_date
                        existing_data[13], # is_active
                        update['effective_date']    # effective_date
                    ]
                    
                    insert_sql = """
                    INSERT INTO hr.dim_employee (
                        employee_id, first_name, last_name, full_name, email_address, phone_number,
                        birth_date, gender, ethnicity, nationality, marital_status,
                        address_line1, address_line2, city, state_province, postal_code, country_code,
                        hire_date, termination_date, is_active, effective_date, expiry_date, is_current,
                        created_date, updated_date
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, '9999-12-31'::date, TRUE,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    """
                    
                    self.cursor.execute(insert_sql, insert_params)
                    count += 1
                    logger.info(f"Updated address for employee: {update['employee_id']}")
                    
            except psycopg2.Error as e:
                logger.error(f"Failed to update address for employee {update['employee_id']}: {e}")
                raise
                
        return count
        
    def _update_employee_names_addresses(self, name_updates: List[Dict]) -> int:
        """Update employee names and addresses using SCD Type II logic"""
        count = 0
        
        for update in name_updates:
            try:
                # Step 1: Close current record
                close_sql = """
                UPDATE hr.dim_employee 
                SET expiry_date = %s - INTERVAL '1 day',
                    is_current = FALSE,
                    updated_date = CURRENT_TIMESTAMP
                WHERE employee_id = %s 
                  AND is_current = TRUE
                """
                
                self.cursor.execute(close_sql, (update['effective_date'], update['employee_id']))
                
                # Step 2: Get existing employee data for the new record
                select_sql = """
                SELECT employee_id, first_name, email_address, phone_number,
                       birth_date, gender, ethnicity, nationality, marital_status,
                       hire_date, termination_date, is_active
                FROM hr.dim_employee 
                WHERE employee_id = %s 
                  AND expiry_date = %s - INTERVAL '1 day'
                ORDER BY employee_key DESC 
                LIMIT 1
                """
                
                self.cursor.execute(select_sql, (update['employee_id'], update['effective_date']))
                existing_data = self.cursor.fetchone()
                
                if existing_data:
                    # Step 3: Insert new record with updated name and address
                    # existing_data order: employee_id, first_name, email_address, phone_number,
                    #                      birth_date, gender, ethnicity, nationality, marital_status,
                    #                      hire_date, termination_date, is_active
                    new_full_name = f"{existing_data[1]} {update['last_name']}"
                    
                    insert_params = [
                        existing_data[0],  # employee_id
                        existing_data[1],  # first_name
                        update['last_name'],        # new last_name
                        new_full_name,              # new full_name
                        existing_data[2],  # email_address
                        existing_data[3],  # phone_number
                        existing_data[4],  # birth_date
                        existing_data[5],  # gender
                        existing_data[6],  # ethnicity
                        existing_data[7],  # nationality
                        existing_data[8],  # marital_status
                        update['address_line1'],    # new address_line1
                        update['address_line2'],    # new address_line2
                        update['city'],             # new city
                        update['state_province'],   # new state_province
                        update['postal_code'],      # new postal_code
                        update['country_code'],     # new country_code
                        existing_data[9],  # hire_date
                        existing_data[10], # termination_date
                        existing_data[11], # is_active
                        update['effective_date']    # effective_date
                    ]
                    
                    insert_sql = """
                    INSERT INTO hr.dim_employee (
                        employee_id, first_name, last_name, full_name, email_address, phone_number,
                        birth_date, gender, ethnicity, nationality, marital_status,
                        address_line1, address_line2, city, state_province, postal_code, country_code,
                        hire_date, termination_date, is_active, effective_date, expiry_date, is_current,
                        created_date, updated_date
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, '9999-12-31'::date, TRUE,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    """
                    
                    self.cursor.execute(insert_sql, insert_params)
                    count += 1
                    logger.info(f"Updated name and address for employee: {update['employee_id']}")
                    
            except psycopg2.Error as e:
                logger.error(f"Failed to update name/address for employee {update['employee_id']}: {e}")
                raise
                
        return count
        
    def validate_data_quality(self) -> bool:
        """
        Validate data quality after ETL process
        
        Returns:
            bool: True if data quality checks pass
        """
        try:
            # Check for duplicate current records
            dup_check_sql = """
            SELECT employee_id, COUNT(*) 
            FROM hr.dim_employee 
            WHERE is_current = TRUE 
            GROUP BY employee_id 
            HAVING COUNT(*) > 1
            """
            
            self.cursor.execute(dup_check_sql)
            duplicates = self.cursor.fetchall()
            
            if duplicates:
                logger.error(f"Data quality issue: Found {len(duplicates)} employees with multiple current records")
                for emp_id, count in duplicates:
                    logger.error(f"  Employee {emp_id}: {count} current records")
                return False
                
            # Check for records with invalid date ranges
            date_check_sql = """
            SELECT employee_id, effective_date, expiry_date
            FROM hr.dim_employee 
            WHERE effective_date >= expiry_date
            """
            
            self.cursor.execute(date_check_sql)
            invalid_dates = self.cursor.fetchall()
            
            if invalid_dates:
                logger.error(f"Data quality issue: Found {len(invalid_dates)} records with invalid date ranges")
                return False
                
            logger.info("Data quality validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Data quality validation failed: {e}")
            return False

def main():
    """Main ETL execution function"""
    if len(sys.argv) != 2:
        print("Usage: python3 python_employee_etl.py <json_file_path>")
        print("Example: python3 python_employee_etl.py employee_20250813.json")
        sys.exit(1)
        
    json_file_path = sys.argv[1]
    
    # Initialize ETL processor
    etl = EmployeeETL()
    
    try:
        # Step 1: Connect to database
        if not etl.connect_to_database():
            logger.error("Failed to connect to database. Exiting.")
            sys.exit(1)
            
        # Step 2: Parse JSON data
        logger.info("Starting ETL process...")
        parsed_data = etl.parse_employee_data(json_file_path)
        
        # Step 3: Process employee data
        if etl.set_employee(parsed_data):
            # Step 4: Validate data quality
            if etl.validate_data_quality():
                logger.info("ETL process completed successfully with data quality validation passed!")
            else:
                logger.warning("ETL process completed but data quality issues detected!")
        else:
            logger.error("ETL process failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"ETL process failed with error: {e}")
        sys.exit(1)
    finally:
        etl.close_connection()

if __name__ == "__main__":
    main()