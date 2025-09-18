import pyodbc
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import re

# Load environment variables from .env
load_dotenv()
AZURE_SQL_CONNECTION_STRING = os.getenv('AZURE_SQL_CONNECTION_STRING')

# Debug: Verify environment variable loading
print("Debug: Environment variable AZURE_SQL_CONNECTION_STRING:")
print(AZURE_SQL_CONNECTION_STRING)
if not AZURE_SQL_CONNECTION_STRING:
    raise ValueError("Error: AZURE_SQL_CONNECTION_STRING is not set or empty.")

# Parse connection string
params = {}
for param in AZURE_SQL_CONNECTION_STRING.split(';'):
    if '=' in param:
        key, value = param.split('=', 1)
        params[key.lower()] = value
server = params.get('server', '')
database = params.get('database', '')
user = params.get('user', params.get('user id', ''))
password = params.get('password', '')

# Debug: Verify parsed connection parameters
print(f"Debug: Parsed connection parameters:")
print(f"  Server: {server}")
print(f"  Database: {database}")
print(f"  User: {user}")
print(f"  Password: {'*' * len(password)} (length: {len(password)})")

# Debug: List available ODBC drivers
try:
    drivers = pyodbc.drivers()
    print("Debug: Available ODBC drivers:")
    for driver in drivers:
        print(f"  - {driver}")
    if "ODBC Driver 18 for SQL Server" not in drivers:
        raise ValueError("Error: ODBC Driver 18 for SQL Server not found.")
except Exception as e:
    print(f"Error: Failed to list ODBC drivers: {str(e)}")
    raise

def derive_initials(first_name):
    """
    Derive initials from FirstName and OtherName.
    """
    initials = []
    if pd.notna(first_name) and isinstance(first_name, str) and first_name.strip():
        name_parts = [part.strip() for part in first_name.split() if part.strip()]
        for part in name_parts:
            if part and part[0].isalpha():
                initials.append(part[0].upper())
    return ''.join(initials)

def derive_dob_from_id(id_number):
    """
    Derive date of birth from a South African ID number.
    """
    if not id_number or not isinstance(id_number, str) or not re.match(r'^\d{13}$', id_number):
        return None
    try:
        year_short = int(id_number[:2])
        month = int(id_number[2:4])
        day = int(id_number[4:6])
        current_year = datetime.now().year % 100
        century = 1900 if year_short > current_year else 2000
        year = century + year_short
        dob = datetime(year, month, day)
        return dob.strftime('%Y-%m-%d')
    except (ValueError, IndexError):
        return None

# Step 1: Define file path
excel_file = '2025-05-13 BeneficialOwnership_Trust_merged_latest.xlsx'

# Step 2: Read the Excel spreadsheet
df = pd.read_excel(excel_file, header=0, dtype=str)
df.columns = df.columns.str.strip()

# Step 3: Rename columns to match schema
rename_dict = {
    'File Number': 'TrustRegNumber',
    'Trust Name': 'TrustName',
    'Tax Reference Number': 'TaxNumber',
    'Tax No': 'TaxReferenceNumber',
    'Masters Office where the trust is registered': 'MastersOffice',
    'Type': 'NatureOfPerson',
    'Full Names / Entity Name': 'FirstName',
    'Surname': 'LastName',
    'ID Type': 'IdentificationType',
    'ID Number / Passport Number/ Registration Number': 'IDNumber',
    'Nationality': 'PassportCountry',
    'E-Mail Address': 'Email',
    'Contact Number (cellphone)': 'CellNumber',
}
df = df.rename(columns=rename_dict)

# Standardize PassportCountry to ISO 3166-1 alpha-2 codes
country_code_map = {
    'south africa': 'ZA',
    'south african': 'ZA',
    'rsa': 'ZA',
    'za': 'ZA',
    'greek': 'GR',
    'greece': 'GR',
    'united kingdom': 'GB',
    'united states': 'US',
    'australia': 'AU',
    'canada': 'CA',
}
df['PassportCountry'] = df['PassportCountry'].str.lower().map(country_code_map).fillna('')
unmapped_countries = df[df['PassportCountry'].isna() & df['PassportCountry'].notna()]['PassportCountry'].unique()
if len(unmapped_countries) > 0:
    print("Warning: Unmapped country values found:", unmapped_countries)

# Standardize MastersOffice values
df['MastersOffice'] = df['MastersOffice'].str.strip().str.lower().replace({
    'pretoria': 'PTA',
    'capetown': 'CPT'
})

# Handle entity vs. natural person distinctions
df.loc[df['NatureOfPerson'].str.contains('entity|company', case=False, na=False), 'CompanyRegisteredName'] = df['FirstName']
df.loc[df['NatureOfPerson'].str.contains('entity|company', case=False, na=False), 'FirstName'] = None

# Step 4: Address parsing function
def parse_address(addr, prefix):
    if pd.isna(addr) or addr is None:
        return {}
    parts = [p.strip() for p in re.split(r'\n', str(addr)) if p.strip()]
    parsed = {}
    num_parts = len(parts)
    skip_list = [
        'GAUTENG', 'KWAZULU-NATAL', 'WESTERN CAPE', 'EASTERN CAPE', 'NORTHERN CAPE',
        'FREE STATE', 'LIMPOPO', 'MPUMALANGA', 'NORTH WEST', 'SOUTH AFRICA', 'SA'
    ]
    while num_parts > 0 and parts[-1].upper() in skip_list:
        parts.pop(-1)
        num_parts -= 1
    if num_parts > 0 and parts[-1].isdigit() and len(parts[-1]) == 4:
        parsed[f'{prefix}PostalCode' if prefix == 'Physical' else f'{prefix}Code'] = parts.pop(-1)
        num_parts -= 1
    if prefix == 'Physical':
        if num_parts >= 1:
            parsed[f'{prefix}City'] = parts.pop(-1)
            num_parts -= 1
        if num_parts >= 1:
            parsed[f'{prefix}Suburb'] = parts.pop(-1)
            num_parts -= 1
        if num_parts > 0:
            street = ' '.join(parts)
            parsed[f'{prefix}Street'] = street
            match = re.match(r'^(\d+[A-Z]?)\\s*(.*)', street)
            if match:
                parsed[f'{prefix}StreetNumber'] = match.group(1)
                parsed[f'{prefix}Street'] = match.group(2)
            if 'UNIT' in street.upper() or 'PLOT' in street.upper():
                parsed[f'{prefix}UnitNumber'] = street
            elif 'COMPLEX' in street.upper():
                parsed[f'{prefix}Complex'] = street
    if len(parts) > 4:
        parts[3] = ' '.join([parts[3]] + parts[4:])
        parts = parts[:4]
    for i, part in enumerate(parts, 1):
        parsed[f'{prefix}AddressLine{i}'] = part
    print(f"Debug: Parsed {prefix} address from '{addr}': {parsed}")
    return parsed

# Apply address parsing
for idx, row in df.iterrows():
    print(f"Debug: Processing row {idx} addresses")
    physical_parsed = parse_address(row.get('Residential Address'), 'Physical')
    postal_parsed = parse_address(
        row.get('Domicilium Address') or row.get('Postal Address of Representative / Guardian'), 'Postal')
    df.loc[idx, list(physical_parsed.keys())] = list(physical_parsed.values())
    df.loc[idx, list(postal_parsed.keys())] = list(postal_parsed.values())
    print(f"Debug: Row {idx} parsed physical: {physical_parsed}")
    print(f"Debug: Row {idx} parsed postal: {postal_parsed}")

# Derive PostalSameAsPhysical
df['PostalSameAsPhysical'] = df.apply(
    lambda row: 1 if str(row.get('Domicilium Address')) == str(row.get('Residential Address')) else 0, axis=1)

# Step 5: Parse boolean flags
def set_flags(grounds, tax_ref):
    flags = {
        'IsConnectedPerson': 0,
        'IsBeneficiary': 0,
        'IsFounder': 0,
        'IsNaturalPerson': 1 if pd.isna(tax_ref) else 0,
        'IsDonor': 0,
        'IsNonResident': 0,
        'IsTaxableOnDistributed': 0,
        'HasNonTaxableAmounts': 0,
        'HasCapitalDistribution': 0,
        'HasLoansGranted': 0,
        'HasLoansFrom': 0,
        'MadeDonations': 0,
        'MadeContributions': 0,
        'ReceivedDonations': 0,
        'ReceivedContributions': 0,
        'MadeDistributions': 0,
        'ReceivedRefunds': 0,
        'HasRightOfUse': 0
    }
    if pd.notna(grounds):
        grounds_lower = str(grounds).lower()
        if 'beneficial owner' in grounds_lower:
            flags['IsBeneficiary'] = 1
        if 'founder' in grounds_lower:
            flags['IsFounder'] = 1
        if 'donor' in grounds_lower:
            flags['IsDonor'] = 1
        if 'loan' in grounds_lower:
            flags['HasLoansGranted'] = 1
            flags['HasLoansFrom'] = 1
        if 'donation' in grounds_lower:
            flags['MadeDonations'] = 1
            flags['ReceivedDonations'] = 1
        if 'contribution' in grounds_lower:
            flags['MadeContributions'] = 1
            flags['ReceivedContributions'] = 1
        if 'distribution' in grounds_lower:
            flags['MadeDistributions'] = 1
        if 'refund' in grounds_lower:
            flags['ReceivedRefunds'] = 1
        if 'right of use' in grounds_lower:
            flags['HasRightOfUse'] = 1
    return flags

df_flags = df.apply(
    lambda row: set_flags(row['Grounds on which the person is a beneficial owner of the trust'], row['TaxReferenceNumber']),
    axis=1
)
df = df.join(pd.DataFrame(df_flags.tolist()))

# Step 6: Connect to Azure SQL Database
print("Debug: Attempting connection with pyodbc...")
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={user};"
    f"PWD={password};"
    f"Encrypt=yes;TrustServerCertificate=no;"
)
try:
    conn = pyodbc.connect(conn_str)
    print("Debug: Connection successful with pyodbc!")
    cursor = conn.cursor()
    print("Debug: Cursor created successfully.")
except pyodbc.Error as e:
    print(f"Error: pyodbc connection failed: {str(e)}")
    raise

# Step 7: Drop existing tables and constraints
tables = ['BeneficiaryDNT', 'BeneficiaryTAD', 'BeneficiaryTFF', 'FinancialTransactions', 'Submissions', 'HGHHeaders', 'Beneficiaries', 'Trusts']
print("Debug: Dropping existing tables and constraints...")
for table in tables:
    try:
        if table in ['Beneficiaries', 'Trusts']:
            cursor.execute(f"""
                DECLARE @sql NVARCHAR(MAX) = '';
                SELECT @sql += 'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(fk.parent_object_id)) + '.' +
                               QUOTENAME(OBJECT_NAME(fk.parent_object_id)) +
                               ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
                FROM sys.foreign_keys fk
                WHERE fk.referenced_object_id = OBJECT_ID('{table}');
                EXEC sp_executesql @sql;
            """)
            print(f"Debug: Dropped foreign key constraints for {table}")
        cursor.execute(f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}")
        print(f"Debug: Dropped table {table}")
    except pyodbc.Error as e:
        print(f"Error: Failed to drop table {table}: {str(e)}")
        raise

# Step 8: Create tables
cursor.execute("""
    CREATE TABLE Trusts (
        TrustID INT IDENTITY(1,1) PRIMARY KEY,
        TrustRegNumber VARCHAR(30) NOT NULL,
        TrustName VARCHAR(120) NOT NULL,
        TaxNumber VARCHAR(10),
        Status VARCHAR(50),
        ReportingDate VARCHAR(10),
        NatureOfPerson VARCHAR(33),
        TrustType VARCHAR(50),
        Residency VARCHAR(50),
        MastersOffice VARCHAR(50),
        PhysicalUnitNumber VARCHAR(50),
        PhysicalComplex VARCHAR(50),
        PhysicalStreetNumber VARCHAR(50),
        PhysicalStreet VARCHAR(50),
        PhysicalSuburb VARCHAR(50),
        PhysicalCity VARCHAR(50),
        PhysicalPostalCode VARCHAR(10),
        PostalSameAsPhysical BIT,
        PostalAddressLine1 VARCHAR(100),
        PostalAddressLine2 VARCHAR(100),
        PostalAddressLine3 VARCHAR(100),
        PostalAddressLine4 VARCHAR(100),
        PostalCode VARCHAR(10),
        ContactNumber VARCHAR(15),
        CellNumber VARCHAR(15),
        Email VARCHAR(80),
        SubmissionTaxYear VARCHAR(4),
        PeriodStartDate VARCHAR(10),
        PeriodEndDate VARCHAR(10),
        UniqueFileID VARCHAR(64),
        UniqueRegistrationNumber VARCHAR(30),
        DateRegisteredMastersOffice VARCHAR(10),
        RecordStatus VARCHAR(50)
    )
""")
cursor.execute("""
    CREATE TABLE Beneficiaries (
        BeneficiaryID INT IDENTITY(1,1) PRIMARY KEY,
        TrustID INT NOT NULL,
        TaxReferenceNumber VARCHAR(30),
        LastName VARCHAR(120) NOT NULL,
        FirstName VARCHAR(100) NOT NULL,
        OtherName VARCHAR(100),
        Initials VARCHAR(30),
        DateOfBirth VARCHAR(10),
        IDNumber VARCHAR(30),
        IdentificationType VARCHAR(3) DEFAULT '001' NOT NULL,
        PassportNumber VARCHAR(30),
        PassportCountry VARCHAR(2),
        PassportIssueDate VARCHAR(10),
        CompanyRegistrationNumber VARCHAR(30),
        CompanyRegisteredName VARCHAR(120),
        NatureOfPerson VARCHAR(33),
        IsConnectedPerson BIT,
        IsBeneficiary BIT,
        IsFounder BIT,
        IsNaturalPerson BIT,
        IsDonor BIT,
        IsNonResident BIT,
        IsTaxableOnDistributed BIT,
        HasNonTaxableAmounts BIT,
        HasCapitalDistribution BIT,
        HasLoansGranted BIT,
        HasLoansFrom BIT,
        MadeDonations BIT,
        MadeContributions BIT,
        ReceivedDonations BIT,
        ReceivedContributions BIT,
        MadeDistributions BIT,
        ReceivedRefunds BIT,
        HasRightOfUse BIT,
        PhysicalUnitNumber VARCHAR(50),
        PhysicalComplex VARCHAR(50),
        PhysicalStreetNumber VARCHAR(50),
        PhysicalStreet VARCHAR(50),
        PhysicalSuburb VARCHAR(50),
        PhysicalCity VARCHAR(50),
        PhysicalPostalCode VARCHAR(10),
        PostalSameAsPhysical BIT,
        PostalAddressLine1 VARCHAR(100),
        PostalAddressLine2 VARCHAR(100),
        PostalAddressLine3 VARCHAR(100),
        PostalAddressLine4 VARCHAR(100),
        PostalCode VARCHAR(10),
        ContactNumber VARCHAR(15),
        CellNumber VARCHAR(15),
        Email VARCHAR(80),
        CompanyIncomeTaxRefNo VARCHAR(10),
        UniqueRecordID VARCHAR(36),
        SequenceNumber INT,
        LinkedRecordID VARCHAR(36),
        RecordStatus VARCHAR(50),
        CONSTRAINT FK_Beneficiaries_Trusts FOREIGN KEY (TrustID) REFERENCES Trusts(TrustID) ON DELETE CASCADE
    )
""")
cursor.execute("""
    CREATE TABLE BeneficiaryDNT (
        DNTID INT IDENTITY(1,1) PRIMARY KEY,
        SectionIdentifier VARCHAR(1) DEFAULT 'B' NOT NULL,
        RecordType VARCHAR(3) DEFAULT 'DNT' NOT NULL,
        RecordStatus VARCHAR(1),
        UniqueNumber VARCHAR(36),
        RowNumber INT,
        BeneficiaryID INT,
        LocalDividends DECIMAL,
        ExemptForeignDividends DECIMAL,
        OtherNonTaxableIncome DECIMAL,
        TrustID INT
    )
""")
cursor.execute("""
    CREATE TABLE BeneficiaryTAD (
        TADID INT IDENTITY(1,1) PRIMARY KEY,
        SectionIdentifier VARCHAR(1) DEFAULT 'B' NOT NULL,
        RecordType VARCHAR(3) DEFAULT 'TAD' NOT NULL,
        RecordStatus VARCHAR(1) NOT NULL,
        UniqueNumber VARCHAR(36) NOT NULL,
        RowNumber INT NOT NULL,
        BeneficiaryID INT NOT NULL,
        AmountSubjectToTax DECIMAL NOT NULL,
        SourceCode VARCHAR(10) NOT NULL,
        ForeignTaxCredits DECIMAL,
        TrustID INT,
        CONSTRAINT FK_BeneficiaryTAD_Beneficiaries FOREIGN KEY (BeneficiaryID) REFERENCES Beneficiaries(BeneficiaryID)
    )
""")
cursor.execute("""
    CREATE TABLE BeneficiaryTFF (
        TFFID INT IDENTITY(1,1) PRIMARY KEY,
        SectionIdentifier VARCHAR(1) DEFAULT 'B' NOT NULL,
        RecordType VARCHAR(3) DEFAULT 'TFF' NOT NULL,
        RecordStatus VARCHAR(1),
        UniqueNumber VARCHAR(36),
        RowNumber INT,
        BeneficiaryID INT,
        TotalValueOfCapitalDistributed DECIMAL,
        TotalExpensesIncurred DECIMAL,
        TotalDonationsToTrust DECIMAL,
        TotalContributionsToTrust DECIMAL,
        TotalDonationsReceivedFromTrust DECIMAL,
        TotalContributionsReceivedFromTrust DECIMAL,
        TotalDistributionsToTrust DECIMAL,
        TotalContributionsRefundedByTrust DECIMAL,
        TrustID INT
    )
""")
cursor.execute("""
    CREATE TABLE HGHHeaders (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        SectionIdentifier VARCHAR(50),
        HeaderType VARCHAR(50),
        MessageCreateDate VARCHAR(20),
        FileLayoutVersion VARCHAR(20),
        UniqueFileID VARCHAR(50),
        SARSRequestReference VARCHAR(50),
        TestDataIndicator VARCHAR(10),
        DataTypeSupplied VARCHAR(50),
        ChannelIdentifier VARCHAR(50),
        SourceIdentifier VARCHAR(50),
        SourceSystem VARCHAR(50),
        SourceSystemVersion VARCHAR(20),
        ContactPersonName VARCHAR(100),
        ContactPersonSurname VARCHAR(100),
        BusinessTelephoneNumber1 VARCHAR(20),
        BusinessTelephoneNumber2 VARCHAR(20),
        CellPhoneNumber VARCHAR(20),
        ContactEmail VARCHAR(100)
    )
""")
cursor.execute("""
    CREATE TABLE Submissions (
        SubmissionID INT IDENTITY(1,1) PRIMARY KEY,
        TrustID INT NOT NULL,
        SubmissionDate VARCHAR(20),
        SubmissionType VARCHAR(50),
        Status VARCHAR(50),
        SoftwareName VARCHAR(50),
        SoftwareVersion VARCHAR(20),
        UserFirstName VARCHAR(100),
        UserLastName VARCHAR(100),
        UserContactNumber VARCHAR(20),
        UserEmail VARCHAR(100),
        SecurityToken VARCHAR(50),
        TotalRecordCount INT,
        MD5Hash VARCHAR(50),
        TotalAmount DECIMAL,
        CONSTRAINT FK_Submissions_Trusts FOREIGN KEY (TrustID) REFERENCES Trusts(TrustID) ON DELETE CASCADE
    )
""")
cursor.execute("""
    CREATE UNIQUE INDEX sqlite_autoindex_Trusts_1 ON Trusts(TrustRegNumber)
""")
print("Debug: All tables created successfully.")

# Step 9: Extract and insert unique Trusts
trust_columns = [
    'TrustRegNumber', 'TrustName', 'TaxNumber', 'Status', 'ReportingDate', 'NatureOfPerson', 'TrustType',
    'Residency', 'MastersOffice', 'PhysicalUnitNumber', 'PhysicalComplex', 'PhysicalStreetNumber',
    'PhysicalStreet', 'PhysicalSuburb', 'PhysicalCity', 'PhysicalPostalCode', 'PostalSameAsPhysical',
    'PostalAddressLine1', 'PostalAddressLine2', 'PostalAddressLine3', 'PostalAddressLine4', 'PostalCode',
    'ContactNumber', 'CellNumber', 'Email', 'SubmissionTaxYear', 'PeriodStartDate', 'PeriodEndDate',
    'UniqueFileID', 'UniqueRegistrationNumber', 'DateRegisteredMastersOffice', 'RecordStatus'
]
df_trusts = df[df['TrustRegNumber'].notnull()].drop_duplicates(subset=['TrustRegNumber'], keep='first')

# Ensure all columns exist, fill missing with None
for col in trust_columns:
    if col not in df_trusts.columns:
        df_trusts[col] = None

# Set default values
df_trusts['SubmissionTaxYear'] = '2025'
df_trusts['PeriodStartDate'] = '2024-03-01'
df_trusts['PeriodEndDate'] = '2025-02-28'
df_trusts['Residency'] = 'ZA'
df_trusts['RecordStatus'] = '0000 - Imported'
df_trusts['ContactNumber'] = '0123428393'
df_trusts['CellNumber'] = df_trusts['CellNumber'].apply(
    lambda x: '0606868076' if pd.isna(x) or len(str(x).strip()) < 1 else str(x).zfill(10)
)

# Cleanse data: Convert empty strings to None and truncate to schema lengths
length_limits = {
    'TrustRegNumber': 30, 'TrustName': 120, 'TaxNumber': 10, 'Status': 50, 'ReportingDate': 10,
    'NatureOfPerson': 33, 'TrustType': 50, 'Residency': 50, 'MastersOffice': 50,
    'PhysicalUnitNumber': 50, 'PhysicalComplex': 50, 'PhysicalStreetNumber': 50, 'PhysicalStreet': 50,
    'PhysicalSuburb': 50, 'PhysicalCity': 50, 'PhysicalPostalCode': 10, 'PostalAddressLine1': 100,
    'PostalAddressLine2': 100, 'PostalAddressLine3': 100, 'PostalAddressLine4': 100, 'PostalCode': 10,
    'ContactNumber': 15, 'CellNumber': 15, 'Email': 80, 'SubmissionTaxYear': 4, 'PeriodStartDate': 10,
    'PeriodEndDate': 10, 'UniqueFileID': 64, 'UniqueRegistrationNumber': 30, 'DateRegisteredMastersOffice': 10,
    'RecordStatus': 50
}
for col in trust_columns:
    df_trusts[col] = df_trusts[col].apply(
        lambda x: None if isinstance(x, str) and x.strip() == '' else x[:length_limits.get(col, len(x))] if isinstance(x, str) else x
    )

# Convert text fields to uppercase
for col in df_trusts.select_dtypes(include=['object']).columns:
    df_trusts[col] = df_trusts[col].apply(lambda x: x.upper() if isinstance(x, str) else x)

# Debug: Print trust_columns and sample data
print(f"Debug: Trust columns ({len(trust_columns)}): {trust_columns}")
print("Debug: Sample of df_trusts (first 5 rows):")
print(df_trusts[trust_columns].head(5))

# Insert trusts
trust_id_map = {}
for idx, row in df_trusts.iterrows():
    try:
        values = [row[col] if pd.notna(row[col]) else None for col in trust_columns]
        print(f"Debug: Inserting row {idx} with values: {values}")
        placeholders = ', '.join(['?' for _ in trust_columns])
        cursor.execute(f"INSERT INTO Trusts ({', '.join(trust_columns)}) VALUES ({placeholders})", values)
        cursor.execute("SELECT @@IDENTITY AS LastID")
        trust_id = cursor.fetchone()[0]
        trust_id_map[row['TrustRegNumber']] = trust_id
        print(f"Debug: Inserted TrustID {trust_id} for TrustRegNumber {row['TrustRegNumber']}")
        conn.commit()
    except pyodbc.Error as e:
        print(f"Error: Failed to insert trust row {idx}: {str(e)}")
        print(f"Error values: {values}")
        raise

# Step 10: Insert Beneficiaries
benef_columns = [
    'TrustID', 'TaxReferenceNumber', 'LastName', 'FirstName', 'OtherName', 'Initials', 'DateOfBirth',
    'IDNumber', 'IdentificationType', 'PassportNumber', 'PassportCountry', 'PassportIssueDate',
    'CompanyRegistrationNumber', 'CompanyRegisteredName', 'NatureOfPerson', 'IsConnectedPerson',
    'IsBeneficiary', 'IsFounder', 'IsNaturalPerson', 'IsDonor', 'IsNonResident', 'IsTaxableOnDistributed',
    'HasNonTaxableAmounts', 'HasCapitalDistribution', 'HasLoansGranted', 'HasLoansFrom', 'MadeDonations',
    'MadeContributions', 'ReceivedDonations', 'ReceivedContributions', 'MadeDistributions', 'ReceivedRefunds',
    'HasRightOfUse', 'PhysicalUnitNumber', 'PhysicalComplex', 'PhysicalStreetNumber', 'PhysicalStreet',
    'PhysicalSuburb', 'PhysicalCity', 'PhysicalPostalCode', 'PostalSameAsPhysical', 'PostalAddressLine1',
    'PostalAddressLine2', 'PostalAddressLine3', 'PostalAddressLine4', 'PostalCode', 'ContactNumber',
    'CellNumber', 'Email', 'CompanyIncomeTaxRefNo', 'UniqueRecordID', 'SequenceNumber', 'LinkedRecordID',
    'RecordStatus'
]
df_benef = df[df['LastName'].notnull()].drop_duplicates(
    subset=['LastName', 'FirstName', 'TrustRegNumber', 'IDNumber'], keep='first')

# Derive Initials and DateOfBirth
df_benef['Initials'] = df_benef.apply(lambda row: derive_initials(row['FirstName']), axis=1)
df_benef['DateOfBirth'] = df_benef['IDNumber'].apply(derive_dob_from_id)

# Ensure all columns exist
for col in benef_columns:
    if col not in df_benef.columns:
        df_benef[col] = None

# Set default RecordStatus
df_benef['RecordStatus'] = '0000 - Imported'

# Apply ContactNumber and CellNumber logic
df_benef['ContactNumber'] = df_benef['ContactNumber'].apply(
    lambda x: '0123428393' if pd.isna(x) or len(str(x).strip()) < 1 else str(x).zfill(10)
)
df_benef['CellNumber'] = df_benef['CellNumber'].apply(
    lambda x: '0606868076' if pd.isna(x) or len(str(x).strip()) < 1 else str(x).zfill(10)
)

# Cleanse data: Truncate to schema lengths
benef_length_limits = {
    'TaxReferenceNumber': 30, 'LastName': 120, 'FirstName': 100, 'OtherName': 100, 'Initials': 30,
    'DateOfBirth': 10, 'IDNumber': 30, 'IdentificationType': 3, 'PassportNumber': 30, 'PassportCountry': 2,
    'PassportIssueDate': 10, 'CompanyRegistrationNumber': 30, 'CompanyRegisteredName': 120,
    'NatureOfPerson': 33, 'PhysicalUnitNumber': 50, 'PhysicalComplex': 50, 'PhysicalStreetNumber': 50,
    'PhysicalStreet': 50, 'PhysicalSuburb': 50, 'PhysicalCity': 50, 'PhysicalPostalCode': 10,
    'PostalAddressLine1': 100, 'PostalAddressLine2': 100, 'PostalAddressLine3': 100, 'PostalAddressLine4': 100,
    'PostalCode': 10, 'ContactNumber': 15, 'CellNumber': 15, 'Email': 80, 'CompanyIncomeTaxRefNo': 10,
    'UniqueRecordID': 36, 'LinkedRecordID': 36, 'RecordStatus': 50
}
for col in benef_columns:
    df_benef[col] = df_benef[col].apply(
        lambda x: None if isinstance(x, str) and x.strip() == '' else x[:benef_length_limits.get(col, len(x))] if isinstance(x, str) else x
    )

# Convert text fields to uppercase
for col in df_benef.select_dtypes(include=['object']).columns:
    df_benef[col] = df_benef[col].apply(lambda x: x.upper() if isinstance(x, str) else x)

# Debug: Print benef_columns and sample data
print(f"Debug: Beneficiary columns ({len(benef_columns)}): {benef_columns}")
print("Debug: Sample of df_benef (first 5 rows):")
print(df_benef[benef_columns].head(5))

# Insert beneficiaries
df_benef = df_benef[benef_columns + ['TrustRegNumber']]
for idx, row in df_benef.iterrows():
    trust_reg = row['TrustRegNumber']
    if trust_reg in trust_id_map:
        row['TrustID'] = trust_id_map[trust_reg]
        # insert_cols = ['TrustID'] + benef_columns
        insert_cols = benef_columns
        values = [row[col] if pd.notna(row[col]) else None for col in insert_cols]
        try:
            print(f"Debug: Inserting beneficiary row {idx} with values: {values}")
            placeholders = ', '.join(['?' for _ in insert_cols])
            cursor.execute(f"INSERT INTO Beneficiaries ({', '.join(insert_cols)}) VALUES ({placeholders})", values)
            conn.commit()
            print(f"Debug: Inserted Beneficiary for TrustRegNumber {trust_reg} at row {idx}")
        except pyodbc.Error as e:
            print(f"Error: Failed to insert beneficiary row {idx}: {str(e)}")
            print(f"Error values: {values}")
            raise
    else:
        print(f"Warning: Skipping beneficiary row {idx}: No matching TrustRegNumber {trust_reg}")

# Step 11: Commit and close
conn.commit()
conn.close()
print("Debug: Import completed. Azure SQL Database: SARSIT3tDB")