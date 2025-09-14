import pandas as pd
import sqlite3
from datetime import datetime
import re  # For address parsing


def derive_initials(first_name):
    """
    Derive initials from FirstName and OtherName.
    Extracts the first letter of each name part, combines them, and returns uppercase initials.
    Handles null, empty, or non-string inputs.
    Returns: String of initials (e.g., 'JA' for 'JOHANNES ANDREAS') or empty string if no valid initials.
    """
    initials = []

    # Process FirstName
    if pd.notna(first_name) and isinstance(first_name, str) and first_name.strip():
        # Split by spaces and filter out empty parts
        name_parts = [part.strip() for part in first_name.split() if part.strip()]
        for part in name_parts:
            if part and part[0].isalpha():  # Ensure first character is alphabetic
                initials.append(part[0].upper())

    return ''.join(initials)

def derive_dob_from_id(id_number):
    """
    Derive date of birth from a South African ID number.
    Format: YYMMDD (first 6 digits). Assumes 1900s if year > current year modulo 100, else 2000s.
    Returns: YYYY-MM-DD string or None if invalid.
    """
    if not id_number or not isinstance(id_number, str) or not re.match(r'^\d{13}$', id_number):
        return None
    try:
        year_short = int(id_number[:2])
        month = int(id_number[2:4])
        day = int(id_number[4:6])
        current_year = datetime.now().year % 100  # Last two digits of current year
        century = 1900 if year_short > current_year else 2000
        year = century + year_short
        dob = datetime(year, month, day)
        return dob.strftime('%Y-%m-%d')
    except (ValueError, IndexError):
        return None

# Step 1: Define file and database paths
excel_file = '2025-05-13 BeneficialOwnership_Trust_merged_latest.xlsx'  # Adjust extension if .xls
db_file = 'beneficial_ownership.db'  # Output database file

# Step 2: Read the Excel spreadsheet (assume single sheet; specify sheet_name if multiple)
df = pd.read_excel(excel_file, header=0, dtype=str)  # Read all data as strings to preserve formats like phone numbers
df.columns = df.columns.str.strip()  # Remove any whitespace

# Diagnostic print to verify columns
print("Actual columns in DataFrame:", df.columns.tolist())
print("Unique Nationality values in spreadsheet:", df['Nationality'].unique())

# Step 3: Rename columns to match schema (using primary mappings)
rename_dict = {
    'File Number': 'TrustRegNumber',
    'Trust Name': 'TrustName',
    'Tax Reference Number': 'TaxNumber',  # For trusts
    'Tax No': 'TaxReferenceNumber',  # For beneficiaries (column AD)
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

# Standardize PassportCountry to ISO 3166-1 alpha-2 codes before renaming
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

df = df.rename(columns=rename_dict)

# Standardize MastersOffice values
df['MastersOffice'] = df['MastersOffice'].str.strip().str.lower().replace({
    'pretoria': 'PTA',
    'capetown': 'CPT'
})

# Handle entity vs. natural person distinctions
df.loc[df['NatureOfPerson'].str.contains('entity|company', case=False, na=False), 'CompanyRegisteredName'] = df['FirstName']
df.loc[df['NatureOfPerson'].str.contains('entity|company', case=False, na=False), 'FirstName'] = None

# Step 4: Updated Address parsing function (split by newline, improved mapping)
def parse_address(addr, prefix):
    if pd.isna(addr) or addr is None:
        return {}
    # Split by newlines, strip empty lines
    parts = [p.strip() for p in re.split(r'\n', str(addr)) if p.strip()]
    parsed = {}
    num_parts = len(parts)

    if num_parts == 0:
        return {}

    # Expanded skip list with all SA provinces and country variants (upper case)
    skip_list = [
        'GAUTENG', 'KWAZULU-NATAL', 'WESTERN CAPE', 'EASTERN CAPE', 'NORTHERN CAPE',
        'FREE STATE', 'LIMPOPO', 'MPUMALANGA', 'NORTH WEST', 'SOUTH AFRICA', 'SA'
    ]

    # Loop to remove all trailing province/country (discard without setting)
    while num_parts > 0 and parts[-1].upper() in skip_list:
        parts.pop(-1)
        num_parts -= 1

    # Extract PostalCode if last part is exactly a 4-digit number (SA-specific)
    if num_parts > 0 and parts[-1].isdigit() and len(parts[-1]) == 4:
        parsed[f'{prefix}PostalCode' if prefix == 'Physical' else f'{prefix}Code'] = parts.pop(-1)
        num_parts -= 1

    # Structured assignment only for Physical (schema has structured fields)
    if prefix == 'Physical':
        if num_parts > 0:
            # Assign from end: city (last), suburb (second-last), street (join remaining)
            if num_parts >= 1:
                parsed[f'{prefix}City'] = parts.pop(-1)
                num_parts -= 1
            if num_parts >= 1:
                parsed[f'{prefix}Suburb'] = parts.pop(-1)
                num_parts -= 1
            # Join remaining parts as street (handle extra lines)
            if num_parts > 0:
                street = ' '.join(parts)
                parsed[f'{prefix}Street'] = street
                # Try to split street number
                match = re.match(r'^(\d+[A-Z]?)\s*(.*)', street)
                if match:
                    parsed[f'{prefix}StreetNumber'] = match.group(1)
                    parsed[f'{prefix}Street'] = match.group(2)
                # Check for unit/complex
                if 'UNIT' in street.upper() or 'PLOT' in street.upper():
                    parsed[f'{prefix}UnitNumber'] = street
                elif 'COMPLEX' in street.upper():
                    parsed[f'{prefix}Complex'] = street

    # Always assign to AddressLines (up to 4; join extras to last if >4)
    if len(parts) > 4:
        # Join excess to the 4th line to avoid loss
        parts[3] = ' '.join([parts[3]] + parts[4:])
        parts = parts[:4]
    for i, part in enumerate(parts, 1):
        parsed[f'{prefix}AddressLine{i}'] = part

    print(f"Debug: Parsed {prefix} address from '{addr}': {parsed}")  # Retained debug
    return parsed

# Apply address parsing with debug
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

# Step 5: Parse boolean flags from 'Grounds on which the person is a beneficial owner of the trust'
def set_flags(grounds, tax_number=None):
    flags = {
        'IsConnectedPerson': 0,
        'IsBeneficiary': 0,
        'IsFounder': 0,
        'IsNaturalPerson': 0,
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
        lower_grounds = str(grounds).lower()
        if 'beneficiary' in lower_grounds:
            flags['IsBeneficiary'] = 1
        if 'founder' in lower_grounds:
            flags['IsFounder'] = 1
        if 'donor' in lower_grounds:
            flags['IsDonor'] = 1
        if 'connected' in lower_grounds:
            flags['IsConnectedPerson'] = 1
        if 'non-resident' in lower_grounds:
            flags['IsNonResident'] = 1
    # Set IsNaturalPerson based on TaxNumber
    if pd.notna(tax_number) and str(tax_number).startswith('9'):
        flags['IsNaturalPerson'] = 0
    else:
        flags['IsNaturalPerson'] = 1
    return flags

# Update the apply call to pass TaxNumber
df_flags = df.apply(
    lambda row: set_flags(
        row['Grounds on which the person is a beneficial owner of the trust'],
        row['TaxNumber']
    ),
    axis=1
).apply(pd.Series)
df = pd.concat([df, df_flags], axis=1)

# Step 6: Data cleaning and validation
# Convert boolean columns to int
boolean_cols = [col for col in df.columns if
                col.startswith('Is') or col.startswith('Has') or col in ['MadeDonations', 'MadeContributions',
                                                                         'ReceivedDonations', 'ReceivedContributions',
                                                                         'MadeDistributions', 'ReceivedRefunds',
                                                                         'PostalSameAsPhysical']]
for col in boolean_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

# Handle dates: Convert DD/MM/YY to YYYY-MM-DD
date_cols = ['Date on which the person became a beneficial owner of the trust (DD/MM/YY)',
             'Date on which the person ceased to be a beneficial owner of the trust (DD/MM/YY)']
for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], format='%d/%m/%y', errors='coerce').dt.strftime('%Y-%m-%d')

# Derive DateOfBirth from IDNumber if not already set
df['DateOfBirth'] = df.apply(
    lambda row: derive_dob_from_id(row['IDNumber']) if pd.isna(row.get('DateOfBirth')) else row.get('DateOfBirth'),
    axis=1
)

# Map to schema date fields
df['ReportingDate'] = df['Date on which the person became a beneficial owner of the trust (DD/MM/YY)']
df['PeriodStartDate'] = df['Date on which the person became a beneficial owner of the trust (DD/MM/YY)']
df['PeriodEndDate'] = df['Date on which the person ceased to be a beneficial owner of the trust (DD/MM/YY)']
df['PassportIssueDate'] = None  # If available, map here

# Handle encoding and nulls
df = df.astype(str).replace({'nan': None, 'NaT': None})

# Step 7: Connect to SQLite and create tables
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS Beneficiaries;")
cursor.execute("DROP TABLE IF EXISTS Trusts;")
cursor.execute("DROP TABLE IF EXISTS BeneficiaryDNT;")
cursor.execute("DROP TABLE IF EXISTS BeneficiaryTFF;")
cursor.execute("DROP TABLE IF EXISTS BeneficiaryTAD;")
cursor.execute("DROP TABLE IF EXISTS Submissions;")
cursor.execute("DROP TABLE IF EXISTS HGHHeaders;")
conn.commit()

# Execute updated schema
cursor.executescript("""
CREATE TABLE IF NOT EXISTS BeneficiaryDNT
(
   DNTID integer PRIMARY KEY,
   SectionIdentifier varchar(1) DEFAULT 'B' NOT NULL,
   RecordType varchar(3) DEFAULT 'DNT' NOT NULL,
   RecordStatus varchar(1),
   UniqueNumber varchar(36),
   RowNumber integer,
   BeneficiaryID integer,
   LocalDividends DECIMAL,
   ExemptForeignDividends DECIMAL,
   OtherNonTaxableIncome DECIMAL,
   TrustID integer
);

CREATE TABLE IF NOT EXISTS BeneficiaryTAD
(
   TADID integer PRIMARY KEY,
   SectionIdentifier varchar(1) DEFAULT 'B' NOT NULL,
   RecordType varchar(3) DEFAULT 'TAD' NOT NULL,
   RecordStatus varchar(1) NOT NULL,
   UniqueNumber varchar(36) NOT NULL,
   RowNumber integer NOT NULL,
   BeneficiaryID integer NOT NULL,
   AmountSubjectToTax DECIMAL NOT NULL,
   SourceCode varchar(10) NOT NULL,
   ForeignTaxCredits DECIMAL,
   TrustID integer
);

CREATE TABLE IF NOT EXISTS BeneficiaryTFF
(
   TFFID integer PRIMARY KEY,
   SectionIdentifier varchar(1) DEFAULT 'B' NOT NULL,
   RecordType varchar(3) DEFAULT 'TFF' NOT NULL,
   RecordStatus varchar(1),
   UniqueNumber varchar(36),
   RowNumber integer,
   BeneficiaryID integer,
   TotalValueOfCapitalDistributed DECIMAL,
   TotalExpensesIncurred DECIMAL,
   TotalDonationsToTrust DECIMAL,
   TotalContributionsToTrust DECIMAL,
   TotalDonationsReceivedFromTrust DECIMAL,
   TotalContributionsReceivedFromTrust DECIMAL,
   TotalDistributionsToTrust DECIMAL,
   TotalContributionsRefundedByTrust DECIMAL,
   TrustID integer
);

CREATE TABLE IF NOT EXISTS HGHHeaders
(
   ID integer PRIMARY KEY,
   SectionIdentifier varchar(2000000000),
   HeaderType varchar(2000000000),
   MessageCreateDate varchar(2000000000),
   FileLayoutVersion varchar(2000000000),
   UniqueFileID varchar(2000000000),
   SARSRequestReference varchar(2000000000),
   TestDataIndicator varchar(2000000000),
   DataTypeSupplied varchar(2000000000),
   ChannelIdentifier varchar(2000000000),
   SourceIdentifier varchar(2000000000),
   SourceSystem varchar(2000000000),
   SourceSystemVersion varchar(2000000000),
   ContactPersonName varchar(2000000000),
   ContactPersonSurname varchar(2000000000),
   BusinessTelephoneNumber1 varchar(2000000000),
   BusinessTelephoneNumber2 varchar(2000000000),
   CellPhoneNumber varchar(2000000000),
   ContactEmail varchar(2000000000)
);

CREATE TABLE IF NOT EXISTS Submissions
(
   SubmissionID integer PRIMARY KEY,
   TrustID integer NOT NULL,
   SubmissionDate varchar(2000000000),
   SubmissionType varchar(2000000000),
   Status varchar(2000000000),
   SoftwareName varchar(2000000000),
   SoftwareVersion varchar(2000000000),
   UserFirstName varchar(2000000000),
   UserLastName varchar(2000000000),
   UserContactNumber varchar(2000000000),
   UserEmail varchar(2000000000),
   SecurityToken varchar(2000000000),
   TotalRecordCount integer,
   MD5Hash varchar(2000000000),
   TotalAmount DECIMAL
);

CREATE TABLE IF NOT EXISTS Trusts
(
   TrustID integer PRIMARY KEY,
   TrustRegNumber varchar(2000000000) NOT NULL,
   TrustName varchar(2000000000) NOT NULL,
   TaxNumber varchar(2000000000),
   Status varchar(2000000000),
   ReportingDate varchar(2000000000),
   NatureOfPerson varchar(2000000000),
   TrustType varchar(2000000000),
   Residency varchar(2000000000),
   MastersOffice varchar(2000000000),
   PhysicalUnitNumber varchar(2000000000),
   PhysicalComplex varchar(2000000000),
   PhysicalStreetNumber varchar(2000000000),
   PhysicalStreet varchar(2000000000),
   PhysicalSuburb varchar(2000000000),
   PhysicalCity varchar(2000000000),
   PhysicalPostalCode varchar(2000000000),
   PostalSameAsPhysical integer,
   PostalAddressLine1 varchar(2000000000),
   PostalAddressLine2 varchar(2000000000),
   PostalAddressLine3 varchar(2000000000),
   PostalAddressLine4 varchar(2000000000),
   PostalCode varchar(2000000000),
   ContactNumber varchar(2000000000),
   CellNumber varchar(2000000000),
   Email varchar(2000000000),
   SubmissionTaxYear varchar(2000000000),
   PeriodStartDate varchar(2000000000),
   PeriodEndDate varchar(2000000000),
   UniqueFileID varchar(2000000000),
   UniqueRegistrationNumber varchar(2000000000),
   DateRegisteredMastersOffice varchar(2000000000),
   RecordStatus varchar(50)
);

CREATE TABLE IF NOT EXISTS Beneficiaries
(
   BeneficiaryID integer PRIMARY KEY,
   TrustID integer NOT NULL,
   TaxReferenceNumber varchar(2000000000),
   LastName varchar(2000000000) NOT NULL,
   FirstName varchar(2000000000) NOT NULL,
   OtherName varchar(2000000000),
   Initials varchar(2000000000),
   DateOfBirth varchar(2000000000),
   IDNumber varchar(2000000000),
   IdentificationType varchar(2000000000) DEFAULT '001' NOT NULL,
   PassportNumber varchar(2000000000),
   PassportCountry varchar(2000000000),
   PassportIssueDate varchar(2000000000),
   CompanyRegistrationNumber varchar(2000000000),
   CompanyRegisteredName varchar(2000000000),
   NatureOfPerson varchar(2000000000),
   IsConnectedPerson integer,
   IsBeneficiary integer,
   IsFounder integer,
   IsNaturalPerson integer,
   IsDonor integer,
   IsNonResident integer,
   IsTaxableOnDistributed integer,
   HasNonTaxableAmounts integer,
   HasCapitalDistribution integer,
   HasLoansGranted integer,
   HasLoansFrom integer,
   MadeDonations integer,
   MadeContributions integer,
   ReceivedDonations integer,
   ReceivedContributions integer,
   MadeDistributions integer,
   ReceivedRefunds integer,
   HasRightOfUse integer,
   PhysicalUnitNumber varchar(2000000000),
   PhysicalComplex varchar(2000000000),
   PhysicalStreetNumber varchar(2000000000),
   PhysicalStreet varchar(2000000000),
   PhysicalSuburb varchar(2000000000),
   PhysicalCity varchar(2000000000),
   PhysicalPostalCode varchar(2000000000),
   PostalSameAsPhysical integer,
   PostalAddressLine1 varchar(2000000000),
   PostalAddressLine2 varchar(2000000000),
   PostalAddressLine3 varchar(2000000000),
   PostalAddressLine4 varchar(2000000000),
   PostalCode varchar(2000000000),
   ContactNumber varchar(2000000000),
   CellNumber varchar(2000000000),
   Email varchar(2000000000),
   CompanyIncomeTaxRefNo varchar(2000000000),
   UniqueRecordID varchar(2000000000),
   SequenceNumber integer,
   LinkedRecordID varchar(2000000000),
   RecordStatus varchar(50),
   FOREIGN KEY (TrustID) REFERENCES Trusts(TrustID) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trusts_trustregnumber ON Trusts(TrustRegNumber);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trusts_trustregnumber ON Trusts(TrustRegNumber);
""")

# Step 8: Extract and insert unique Trusts
df_trusts = df[df['TrustRegNumber'].notnull()].drop_duplicates(subset=['TrustRegNumber'], keep='first')

# Define trust columns
trust_columns = [
    'TrustRegNumber', 'TrustName', 'TaxNumber', 'Status', 'ReportingDate', 'NatureOfPerson', 'TrustType', 'Residency',
    'MastersOffice',
    'PhysicalUnitNumber', 'PhysicalComplex', 'PhysicalStreetNumber', 'PhysicalStreet', 'PhysicalSuburb', 'PhysicalCity',
    'PhysicalPostalCode',
    'PostalSameAsPhysical', 'PostalAddressLine1', 'PostalAddressLine2', 'PostalAddressLine3', 'PostalAddressLine4',
    'PostalCode',
    'ContactNumber', 'CellNumber', 'Email', 'SubmissionTaxYear', 'PeriodStartDate', 'PeriodEndDate', 'UniqueFileID',
    'UniqueRegistrationNumber', 'DateRegisteredMastersOffice', 'RecordStatus'
]
# Ensure all columns exist, fill missing with None
for col in trust_columns:
    if col not in df_trusts.columns:
        df_trusts[col] = None

# Set default values for Trusts
df_trusts['SubmissionTaxYear'] = '2025'
df_trusts['PeriodStartDate'] = '2024-03-01'
df_trusts['PeriodEndDate'] = '2025-02-28'
df_trusts['Residency'] = 'ZA'
df_trusts['RecordStatus'] = '0000 - Imported'

# Apply ContactNumber and CellNumber logic for Trusts
df_trusts['ContactNumber'] = '0123428393'  # Hardcode ContactNumber
df_trusts['CellNumber'] = df_trusts['CellNumber'].apply(
    lambda x: '0606868076' if pd.isna(x) or len(str(x).strip()) < 1 else str(x).zfill(10)
)

# Convert all text fields to uppercase
for col in df_trusts.select_dtypes(include=['object']).columns:
    df_trusts[col] = df_trusts[col].apply(lambda x: x.upper() if isinstance(x, str) else x)

# Debug: Print size and sample of df_trusts
print(f"Number of unique trusts in df_trusts: {len(df_trusts)}")

df_trusts = df_trusts[trust_columns]

# Insert trusts and create ID map
trust_id_map = {}
for idx, row in df_trusts.iterrows():
    try:
        placeholders = ', '.join(['?'] * len(row))
        cursor.execute(f"INSERT INTO Trusts ({', '.join(trust_columns)}) VALUES ({placeholders})", tuple(row))
        trust_id = cursor.lastrowid
        trust_id_map[row['TrustRegNumber']] = trust_id
        print(f"Inserted TrustID {trust_id} for TrustRegNumber {row['TrustRegNumber']}")
    except sqlite3.IntegrityError as e:
        print(f"Duplicate or constraint error for TrustRegNumber {row['TrustRegNumber']}: {e}")
    except Exception as e:
        print(f"Error inserting trust row {idx}: {e}")

# Step 9: Insert Beneficiaries
df_benef = df[df['LastName'].notnull()].drop_duplicates(
    subset=['LastName', 'FirstName', 'TrustRegNumber', 'DateOfBirth'], keep='first')

df_benef['Initials'] = df_benef.apply(
    lambda row: derive_initials(row['FirstName']), axis=1
)




# Define beneficiary columns
benef_columns = [
    'TaxReferenceNumber', 'LastName', 'FirstName', 'OtherName', 'Initials', 'DateOfBirth', 'IDNumber',
    'IdentificationType',
    'PassportNumber', 'PassportCountry', 'PassportIssueDate', 'CompanyRegistrationNumber', 'CompanyRegisteredName',
    'NatureOfPerson',
    'IsConnectedPerson', 'IsBeneficiary', 'IsFounder', 'IsNaturalPerson', 'IsDonor', 'IsNonResident',
    'IsTaxableOnDistributed',
    'HasNonTaxableAmounts', 'HasCapitalDistribution', 'HasLoansGranted', 'HasLoansFrom', 'MadeDonations',
    'MadeContributions',
    'ReceivedDonations', 'ReceivedContributions', 'MadeDistributions', 'ReceivedRefunds', 'HasRightOfUse',
    'PhysicalUnitNumber', 'PhysicalComplex', 'PhysicalStreetNumber', 'PhysicalStreet', 'PhysicalSuburb', 'PhysicalCity',
    'PhysicalPostalCode',
    'PostalSameAsPhysical', 'PostalAddressLine1', 'PostalAddressLine2', 'PostalAddressLine3', 'PostalAddressLine4',
    'PostalCode',
    'ContactNumber', 'CellNumber', 'Email', 'CompanyIncomeTaxRefNo', 'UniqueRecordID', 'SequenceNumber',
    'LinkedRecordID', 'RecordStatus'
]
# Ensure all columns exist, fill missing with None
for col in benef_columns:
    if col not in df_benef.columns:
        df_benef[col] = None

# Set default RecordStatus for Beneficiaries
df_benef['RecordStatus'] = '0000 - Imported'

# Apply ContactNumber and CellNumber logic for Beneficiaries
df_benef['ContactNumber'] = df_benef['ContactNumber'].apply(
    lambda x: '0123428393' if pd.isna(x) or len(str(x).strip()) < 1 else str(x).zfill(10)
)
df_benef['CellNumber'] = df_benef['CellNumber'].apply(
    lambda x: '0606868076' if pd.isna(x) or len(str(x).strip()) < 1 else str(x).zfill(10)
)

# Convert all text fields to uppercase
for col in df_benef.select_dtypes(include=['object']).columns:
    df_benef[col] = df_benef[col].apply(lambda x: x.upper() if isinstance(x, str) else x)

# Debug: Print size and sample of df_benef
print(f"Number of beneficiaries in df_benef: {len(df_benef)}")
if not df_benef.empty:
    print("Sample of first 10 rows in df_benef:")
    print(df_benef.head(10))

df_benef = df_benef[benef_columns + ['TrustRegNumber']]  # Temp for mapping

for idx, row in df_benef.iterrows():
    trust_reg = row['TrustRegNumber']
    if trust_reg in trust_id_map:
        row['TrustID'] = trust_id_map[trust_reg]
        insert_cols = ['TrustID'] + benef_columns
        placeholders = ', '.join(['?'] * len(insert_cols))
        try:
            cursor.execute(f"INSERT INTO Beneficiaries ({', '.join(insert_cols)}) VALUES ({placeholders})",
                           tuple(row[col] for col in insert_cols))
            print(f"Inserted Beneficiary for TrustRegNumber {trust_reg} at row {idx}")
        except sqlite3.IntegrityError as e:
            print(f"Constraint error for beneficiary row {idx}: {e}")
        except Exception as e:
            print(f"Error inserting beneficiary row {idx}: {e}")
    else:
        print(f"Skipping beneficiary row {idx}: No matching TrustRegNumber {trust_reg}")

# Step 10: Commit and close
conn.commit()
conn.close()
print("Import completed. Database file:", db_file)

# Additional debug: Fetch and print trusts and PassportCountry values after import
conn = sqlite3.connect(db_file)
trusts = conn.execute(
    "SELECT * FROM Trusts WHERE RecordStatus NOT IN ('9010 - SUBMITTED TO SARS') ORDER BY RecordStatus DESC").fetchall()
print(f"Number of trusts fetched: {len(trusts)}")
if trusts:
    print("Sample of first 10 trusts:")
    for row in trusts[:10]:
        print(row)

beneficiaries = conn.execute("SELECT DISTINCT PassportCountry FROM Beneficiaries").fetchall()
print("Distinct PassportCountry values in Beneficiaries table:", [row[0] for row in beneficiaries])
conn.close()