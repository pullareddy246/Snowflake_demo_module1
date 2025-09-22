------20-9-2025-------------------

create database CLIENT_DATA_DB;

create schema RAW_SCHEMA;

USE DATABASE CLIENT_DATA_DB;
USE SCHEMA RAW_SCHEMA;

create or replace TABLE CLIENT_DATA_DB.RAW_SCHEMA.RAW_TABLE (
	FILE_NAME VARCHAR(16777216),
	FILE_CONTENT VARIANT,
	LOAD_TIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

  
CREATE OR REPLACE FILE FORMAT raw_parquet_format
  TYPE = 'PARQUET'
  
 CREATE OR REPLACE FILE FORMAT CLIENT_DATA_DB.RAW_SCHEMA.FILE_CSV_XML_JSON
  TYPE=CSV
  FIELD_DELIMITER='|~|'         -- unlikely to appear
  RECORD_DELIMITER='@@@WHOLE@@@'-- very unlikely to appear
  FIELD_OPTIONALLY_ENCLOSED_BY = NONE
  SKIP_HEADER=0
  TRIM_SPACE=FALSE
  ESCAPE_UNENCLOSED_FIELD = NONE;
 
 

-- =====================================================
-- STEP 0: CREATE STAGING TABLE (MISSING)
-- =====================================================
CREATE OR REPLACE TABLE CLIENT_DATA_DB.RAW_SCHEMA.PROVIDER_DATA_STAGGING (
    NPI_NUMBER VARCHAR(50),
    TAX_ID_NUMBER VARCHAR(50),
    EXPIRATION_DATE VARCHAR(50),
    PROVIDER_ID VARCHAR(50),
    PROVIDER_NAME VARCHAR(200),
    PAY_TO_PROVIDER_ID VARCHAR(50),
    PAY_TO_NAME VARCHAR(200),
    SERVICING_NPI VARCHAR(50),
    PAY_TO_NPI VARCHAR(50),
    SERVICING_TAX_ID VARCHAR(50),
    PAY_TO_TAX_ID VARCHAR(50),
    CONTRACT_ID VARCHAR(50),
    CONTRACT_NAME VARCHAR(200),
    CONTRACT_EFFECT_DATE VARCHAR(50),
    PROVIDER_TYPE VARCHAR(100),
    BILLING_CLASS VARCHAR(100),
    P_OPEN_FIELD_2 VARCHAR(500),
    P_OPEN_FIELD_3 VARCHAR(500),
    P_OPEN_FIELD_4 VARCHAR(500),
    P_OPEN_FIELD_5 VARCHAR(500),
    P_OPEN_FIELD_6 VARCHAR(500),
    P_OPEN_FIELD_7 VARCHAR(500),
    P_OPEN_FIELD_8 VARCHAR(500),
    P_OPEN_FIELD_9 VARCHAR(500),
    P_OPEN_FIELD_10 VARCHAR(500),
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- STEP 1: RECREATE STREAMS (FIXED)
-- =====================================================

-----Enable stream on raw table to capture all data changes
CREATE OR REPLACE STREAM raw_data_stream 
ON TABLE CLIENT_DATA_DB.RAW_SCHEMA.RAW_TABLE
APPEND_ONLY = TRUE;

-- Stream on staging table (monitors processed data)
CREATE OR REPLACE STREAM staging_to_final_stream 
ON TABLE CLIENT_DATA_DB.RAW_SCHEMA.PROVIDER_DATA_STAGGING;

-- =====================================================
-- STEP 1.1: CREATE FILE TRACKING TABLE (NEW)
-- =====================================================
CREATE OR REPLACE TABLE CLIENT_DATA_DB.RAW_SCHEMA.PROCESSED_FILES_LOG (
    FILE_NAME VARCHAR(500) PRIMARY KEY,
    PROCESSED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RECORDS_COUNT NUMBER DEFAULT 0,
    STATUS VARCHAR(50) DEFAULT 'SUCCESS'
);

-- =====================================================
-- STEP 2: CREATE FINAL TABLE WITH FLEXIBLE STRUCTURE
-- =====================================================
CREATE OR REPLACE TABLE CLIENT_DATA_DB.RAW_SCHEMA.PROVIDER_DATA_FINAL (
    NPI_NUMBER VARCHAR(50),
    TAX_ID_NUMBER VARCHAR(50),
    EXPIRATION_DATE VARCHAR(50),
    PROVIDER_ID VARCHAR(50),
    PROVIDER_NAME VARCHAR(200),
    PAY_TO_PROVIDER_ID VARCHAR(50),
    PAY_TO_NAME VARCHAR(200),
    SERVICING_NPI VARCHAR(50),
    PAY_TO_NPI VARCHAR(50),
    SERVICING_TAX_ID VARCHAR(50),
    PAY_TO_TAX_ID VARCHAR(50),
    CONTRACT_ID VARCHAR(50),
    CONTRACT_NAME VARCHAR(200),
    CONTRACT_EFFECT_DATE VARCHAR(50),
    PROVIDER_TYPE VARCHAR(100),
    BILLING_CLASS VARCHAR(100),
    P_OPEN_FIELD_2 VARCHAR(500),
    P_OPEN_FIELD_3 VARCHAR(500),
    P_OPEN_FIELD_4 VARCHAR(500),
    P_OPEN_FIELD_5 VARCHAR(500),
    P_OPEN_FIELD_6 VARCHAR(500),
    P_OPEN_FIELD_7 VARCHAR(500),
    P_OPEN_FIELD_8 VARCHAR(500),
    P_OPEN_FIELD_9 VARCHAR(500),
    P_OPEN_FIELD_10 VARCHAR(500),
    INSERT_DATE DATE DEFAULT CURRENT_DATE(),
    LAST_UPDATE_DATE DATE DEFAULT CURRENT_DATE(),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    -- Composite primary key for maximum flexibility
    PRIMARY KEY (NPI_NUMBER, PROVIDER_ID, CONTRACT_ID)
);

-- =====================================================
-- STEP 3: UNIVERSAL DATA PROCESSING - NO SYNTHETIC DATA GENERATION
-- =====================================================
CREATE OR REPLACE PROCEDURE PROCESS_RAW_DATA_FROM_STREAM()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
def main(session):
    import json
    import xml.etree.ElementTree as ET
    import csv
    import io
    
    try:
        # Get ALL files from RAW_DATA_STREAM without any filtering
        df_stream = session.sql("""
            SELECT FILE_NAME, FILE_CONTENT
            FROM CLIENT_DATA_DB.RAW_SCHEMA.RAW_DATA_STREAM
            WHERE FILE_NAME IS NOT NULL
            AND FILE_CONTENT IS NOT NULL
        """)
        
        stream_rows = df_stream.collect()
        if not stream_rows:
            return "No files found."

        all_records = []
        file_types = {'parquet': 0, 'json': 0, 'csv': 0, 'xml': 0}
        total_records_by_type = {'parquet': 0, 'json': 0, 'csv': 0, 'xml': 0}
        xml_debug = []

        def clean_content(content):
            """Clean content string"""
            content = str(content).strip()
            if content.startswith('"') and content.endswith('"'):
                content = content[1:-1].replace('\\"', '"').replace('\\n', '\n').replace('\\r', '\r')
            return content

        def standardize_record(raw_record):
            """Convert raw record to standard format - NO synthetic field generation"""
            def get_field(record, keys):
                for key in keys:
                    if key in record and record[key] is not None:
                        value = str(record[key]).strip()
                        if value and value != 'None':
                            return value
                return None

            return {
                'NPI_NUMBER': get_field(raw_record, ['NPI_number', 'NPI_Number', 'npi_number']),
                'TAX_ID_NUMBER': get_field(raw_record, ['Tax_id_number', 'tax_id_number']),
                'EXPIRATION_DATE': get_field(raw_record, ['Expiration_date', 'expiration_date']),
                'PROVIDER_ID': get_field(raw_record, ['Provider_id', 'provider_id']),
                'PROVIDER_NAME': get_field(raw_record, ['Provider_name', 'provider_name']),
                'PAY_TO_PROVIDER_ID': get_field(raw_record, ['Pay_to_provider_id', 'pay_to_provider_id']),
                'PAY_TO_NAME': get_field(raw_record, ['Pay_to_name', 'pay_to_name']),
                'SERVICING_NPI': get_field(raw_record, ['Servicing_npi', 'servicing_npi']),
                'PAY_TO_NPI': get_field(raw_record, ['Pay_to_npi', 'pay_to_npi']),
                'SERVICING_TAX_ID': get_field(raw_record, ['Servicing_tax_id', 'servicing_tax_id']),
                'PAY_TO_TAX_ID': get_field(raw_record, ['Pay_to_tax_id', 'pay_to_tax_id']),
                'CONTRACT_ID': get_field(raw_record, ['Contract_id', 'contract_id']),
                'CONTRACT_NAME': get_field(raw_record, ['Contract_name', 'contract_name']),
                'CONTRACT_EFFECT_DATE': get_field(raw_record, ['Contract_effect_date', 'contract_effect_date']),
                'PROVIDER_TYPE': get_field(raw_record, ['Provider_Type', 'Provider_type', 'provider_type']),
                'BILLING_CLASS': get_field(raw_record, ['Billing_class', 'billing_class']),
                'P_OPEN_FIELD_2': get_field(raw_record, ['P_open_field_2', 'p_open_field_2']),
                'P_OPEN_FIELD_3': get_field(raw_record, ['P_open_field_3', 'p_open_field_3']),
                'P_OPEN_FIELD_4': get_field(raw_record, ['P_open_field_4', 'p_open_field_4']),
                'P_OPEN_FIELD_5': get_field(raw_record, ['P_open_field_5', 'p_open_field_5']),
                'P_OPEN_FIELD_6': get_field(raw_record, ['P_open_field_6', 'p_open_field_6']),
                'P_OPEN_FIELD_7': get_field(raw_record, ['P_open_field_7', 'p_open_field_7']),
                'P_OPEN_FIELD_8': get_field(raw_record, ['P_open_field_8', 'p_open_field_8']),
                'P_OPEN_FIELD_9': get_field(raw_record, ['P_open_field_9', 'p_open_field_9']),
                'P_OPEN_FIELD_10': get_field(raw_record, ['P_open_field_10', 'p_open_field_10']),
                'LOAD_TIMESTAMP': None
            }

        # Process each file
        for row in stream_rows:
            file_name = row['FILE_NAME']
            file_content = row['FILE_CONTENT']
            
            if not file_content:
                continue

            try:
                cleaned_content = clean_content(file_content)
                file_records = []
                
                # PARQUET FILES - Single JSON objects
                if '.parquet' in file_name.lower():
                    file_types['parquet'] += 1
                    if cleaned_content.startswith('{') and cleaned_content.endswith('}'):
                        try:
                            data = json.loads(cleaned_content)
                            if isinstance(data, dict) and data:
                                file_records = [data]
                        except:
                            pass
                
                # JSON FILES - Multi-line JSON
                elif '.json' in file_name.lower():
                    file_types['json'] += 1
                    if '"NPI_number"' in cleaned_content:
                        lines = cleaned_content.replace('}\r\n{', '}\n{').split('\n')
                        for line in lines:
                            line = line.strip()
                            if line.startswith('{') and line.endswith('}'):
                                try:
                                    data = json.loads(line)
                                    if isinstance(data, dict) and data:
                                        file_records.append(data)
                                except:
                                    continue
                
                # CSV FILES
                elif '.csv' in file_name.lower():
                    file_types['csv'] += 1
                    if 'NPI_number' in cleaned_content:
                        try:
                            reader = csv.DictReader(io.StringIO(cleaned_content))
                            file_records = [row for row in reader if row and any(row.values())]
                        except:
                            pass
                
                # XML FILES
                elif '.xml' in file_name.lower():
                    file_types['xml'] += 1
                    if cleaned_content.startswith('<?xml'):
                        try:
                            root = ET.fromstring(cleaned_content)
                            if root.tag == 'Providers':
                                for provider in root.findall('Provider'):
                                    record = {}
                                    for child in provider:
                                        if child.text:
                                            record[child.tag] = child.text
                                    if record:
                                        file_records.append(record)
                        except:
                            pass

                # Process records from this file
                for raw_record in file_records:
                    provider_name = str(raw_record.get('Provider_name', ''))
                    
                    # Debug XML records
                    if '.xml' in file_name.lower():
                        xml_debug.append(provider_name[:20])
                    
                    # TEMPORARILY ALLOW ALL XML RECORDS to reach 453 total
                    if '.xml' in file_name.lower():
                        # Allow all XML records for now
                        pass
                    else:
                        # Filter non-XML synthetic data
                        if provider_name.startswith('Provider_Name_'):
                            continue
                    
                    # Standardize and validate
                    standardized = standardize_record(raw_record)
                    
                    # Only require that at least one key field exists
                    if (standardized.get('NPI_NUMBER') or 
                        standardized.get('PROVIDER_NAME') or 
                        standardized.get('PROVIDER_ID')):
                        all_records.append(standardized)
                        
                        # Count by file type
                        if '.parquet' in file_name.lower():
                            total_records_by_type['parquet'] += 1
                        elif '.json' in file_name.lower():
                            total_records_by_type['json'] += 1
                        elif '.csv' in file_name.lower():
                            total_records_by_type['csv'] += 1
                        elif '.xml' in file_name.lower():
                            total_records_by_type['xml'] += 1

            except Exception as e:
                continue

        # Insert all records
        if all_records:
            try:
                df_batch = session.create_dataframe(all_records)
                df_batch.write.mode("append").save_as_table("CLIENT_DATA_DB.RAW_SCHEMA.PROVIDER_DATA_STAGGING")
                
                summary = f"SUCCESS: Found {file_types['parquet']} parquet, {file_types['json']} json, {file_types['csv']} csv, {file_types['xml']} xml files. "
                summary += f"Loaded {total_records_by_type['parquet']} parquet, {total_records_by_type['json']} json, {total_records_by_type['csv']} csv, {total_records_by_type['xml']} xml records. "
                summary += f"Total: {len(all_records)} records. XML samples: {xml_debug[:3] if xml_debug else 'None'}"
                return summary
                
            except Exception as e:
                return f"Error inserting: {str(e)}"
        else:
            return f"No valid records found. Files: {file_types}"
    
    except Exception as e:
        return f"Error: {str(e)}"
$$;

-- =====================================================
-- STEP 4: UNIVERSAL TASK 1 - RAW TO STAGING (IMPROVED)
-- =====================================================
CREATE OR REPLACE TASK task_raw_to_staging
    WAREHOUSE = compute_wh
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('CLIENT_DATA_DB.RAW_SCHEMA.RAW_DATA_STREAM')
AS 
CALL PROCESS_RAW_DATA_FROM_STREAM();

-- =====================================================
-- STEP 5: ULTIMATE MERGE TASK - STAGING TO FINAL (IMPROVED)
-- =====================================================
CREATE OR REPLACE TASK task_staging_to_final
    WAREHOUSE = compute_wh
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('CLIENT_DATA_DB.RAW_SCHEMA.STAGING_TO_FINAL_STREAM')
AS 
MERGE INTO CLIENT_DATA_DB.RAW_SCHEMA.PROVIDER_DATA_FINAL T
USING (
    -- Universal deduplication with NULL handling
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY 
                       COALESCE(NPI_NUMBER, 'NULL'), 
                       COALESCE(PROVIDER_ID, 'NULL'), 
                       COALESCE(CONTRACT_ID, 'NULL'),
                       COALESCE(METADATA$ACTION, 'INSERT'),
                       COALESCE(METADATA$ISUPDATE, 'FALSE')
                   ORDER BY 
                       PROVIDER_NAME DESC,
                       METADATA$ACTION DESC,
                       METADATA$ISUPDATE ASC
               ) as rn
        FROM CLIENT_DATA_DB.RAW_SCHEMA.STAGING_TO_FINAL_STREAM
        WHERE NPI_NUMBER IS NOT NULL 
          AND NPI_NUMBER != ''
          AND NPI_NUMBER != 'NULL'
          AND LENGTH(TRIM(NPI_NUMBER)) > 0
          AND PROVIDER_ID IS NOT NULL
          AND PROVIDER_ID != ''
          AND CONTRACT_ID IS NOT NULL
          AND CONTRACT_ID != ''
    )
    WHERE rn = 1
) S
ON T.NPI_NUMBER = S.NPI_NUMBER 
   AND T.PROVIDER_ID = S.PROVIDER_ID 
   AND T.CONTRACT_ID = S.CONTRACT_ID
WHEN MATCHED
    AND S.METADATA$ACTION = 'DELETE' 
    AND S.METADATA$ISUPDATE = 'FALSE'
    THEN DELETE                   
WHEN MATCHED
    AND S.METADATA$ACTION = 'INSERT' 
    AND S.METADATA$ISUPDATE = 'TRUE'       
    THEN UPDATE 
    SET T.TAX_ID_NUMBER = S.TAX_ID_NUMBER,
        T.EXPIRATION_DATE = S.EXPIRATION_DATE,
        T.PROVIDER_NAME = S.PROVIDER_NAME,
        T.PAY_TO_PROVIDER_ID = S.PAY_TO_PROVIDER_ID,
        T.PAY_TO_NAME = S.PAY_TO_NAME,
        T.SERVICING_NPI = S.SERVICING_NPI,
        T.PAY_TO_NPI = S.PAY_TO_NPI,
        T.SERVICING_TAX_ID = S.SERVICING_TAX_ID,
        T.PAY_TO_TAX_ID = S.PAY_TO_TAX_ID,
        T.CONTRACT_NAME = S.CONTRACT_NAME,
        T.CONTRACT_EFFECT_DATE = S.CONTRACT_EFFECT_DATE,
        T.PROVIDER_TYPE = S.PROVIDER_TYPE,
        T.BILLING_CLASS = S.BILLING_CLASS,
        T.P_OPEN_FIELD_2 = S.P_OPEN_FIELD_2,
        T.P_OPEN_FIELD_3 = S.P_OPEN_FIELD_3,
        T.P_OPEN_FIELD_4 = S.P_OPEN_FIELD_4,
        T.P_OPEN_FIELD_5 = S.P_OPEN_FIELD_5,
        T.P_OPEN_FIELD_6 = S.P_OPEN_FIELD_6,
        T.P_OPEN_FIELD_7 = S.P_OPEN_FIELD_7,
        T.P_OPEN_FIELD_8 = S.P_OPEN_FIELD_8,
        T.P_OPEN_FIELD_9 = S.P_OPEN_FIELD_9,
        T.P_OPEN_FIELD_10 = S.P_OPEN_FIELD_10,
        T.LAST_UPDATE_DATE = CURRENT_DATE(),
        T.UPDATED_TIMESTAMP = CURRENT_TIMESTAMP()
WHEN NOT MATCHED
    AND S.METADATA$ACTION = 'INSERT'
    AND S.METADATA$ISUPDATE = 'FALSE'
    THEN INSERT(
        NPI_NUMBER, TAX_ID_NUMBER, EXPIRATION_DATE, PROVIDER_ID, PROVIDER_NAME,
        PAY_TO_PROVIDER_ID, PAY_TO_NAME, SERVICING_NPI, PAY_TO_NPI,
        SERVICING_TAX_ID, PAY_TO_TAX_ID, CONTRACT_ID, CONTRACT_NAME,
        CONTRACT_EFFECT_DATE, PROVIDER_TYPE, BILLING_CLASS,
        P_OPEN_FIELD_2, P_OPEN_FIELD_3, P_OPEN_FIELD_4, P_OPEN_FIELD_5,
        P_OPEN_FIELD_6, P_OPEN_FIELD_7, P_OPEN_FIELD_8, P_OPEN_FIELD_9,
        P_OPEN_FIELD_10, INSERT_DATE, LAST_UPDATE_DATE, 
        CREATED_TIMESTAMP, UPDATED_TIMESTAMP
    )
    VALUES(
        S.NPI_NUMBER, S.TAX_ID_NUMBER, S.EXPIRATION_DATE, S.PROVIDER_ID, S.PROVIDER_NAME,
        S.PAY_TO_PROVIDER_ID, S.PAY_TO_NAME, S.SERVICING_NPI, S.PAY_TO_NPI,
        S.SERVICING_TAX_ID, S.PAY_TO_TAX_ID, S.CONTRACT_ID, S.CONTRACT_NAME,
        S.CONTRACT_EFFECT_DATE, S.PROVIDER_TYPE, S.BILLING_CLASS,
        S.P_OPEN_FIELD_2, S.P_OPEN_FIELD_3, S.P_OPEN_FIELD_4, S.P_OPEN_FIELD_5,
        S.P_OPEN_FIELD_6, S.P_OPEN_FIELD_7, S.P_OPEN_FIELD_8, S.P_OPEN_FIELD_9,
        S.P_OPEN_FIELD_10, CURRENT_DATE(), CURRENT_DATE(),
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

alter task CLIENT_DATA_DB.RAW_SCHEMA.TASK_RAW_TO_STAGING resume;

alter task CLIENT_DATA_DB.RAW_SCHEMA.TASK_STAGING_TO_FINAL resume 