import csv
import subprocess
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, make_response
import pyodbc
import os
import io
from datetime import datetime
import uuid
import json
import hashlib
import jsonify
from flask import g

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'fallback-secret-key')

# Define a mapping for IdentificationType conversion
IDENTIFICATION_TYPE_MAPPING = {
    'South African Id': '001',
    'Passport': '002'
}


# Conversion function to use in the route
def convert_identification_type(raw_value):
    return IDENTIFICATION_TYPE_MAPPING.get(raw_value.strip().title(), '')  # Default to empty if invalid


def sanitize(value):
    return '' if value in (None, 'None', 'none') else str(value)


def get_db_connection():
    try:
        connection_params = {
            'Driver': '{ODBC Driver 18 for SQL Server}',
            'Server': 'tcp:jdlsoft-sarsit3t-sql.database.windows.net,1433',
            'Database': 'SARSIT3tDB',
            'UID': 'jdlsoftadmin',
            'PWD': 'f@incC66',
            'Persist Security Info': 'False',
            'MultipleActiveResultSets': 'False',
            'Encrypt': 'yes',
            'TrustServerCertificate': 'no',
        }
        connection = pyodbc.connect(**connection_params)
        return connection
    except pyodbc.Error as e:
        raise Exception(f"Database connection failed: {str(e)}")


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'db'):
        g.db.close()


def dictfetchall(cursor):
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def dictfetchone(cursor):
    row = cursor.fetchone()
    if row is None:
        return None
    columns = [col[0] for col in cursor.description]
    return dict(zip(columns, row))


def modulus_10_check(number):
    if not number.isdigit() or len(number) != 10:
        return False
    digits = list(map(int, number))
    sum_no_double = sum(digits[1::2])
    even_digits = digits[0::2]
    doubled_even = [2 * d if 2 * d < 10 else 2 * d - 9 for d in even_digits]
    sum_double = sum(doubled_even)
    total = sum_no_double + sum_double
    return total % 10 == 0


def sa_id_check(id_number, date_of_birth):
    if not id_number.isdigit() or len(id_number) != 13:
        return False
    if date_of_birth is None:
        return False
    try:
        dob = datetime.strptime(date_of_birth, '%Y-%m-%d')
        yy = f"{dob.year % 100:02d}"
        mm = f"{dob.month:02d}"
        dd = f"{dob.day:02d}"
        expected = yy + mm + dd
        return id_number[:6] == expected
    except ValueError:
        return False


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/beneficiaries/<int:trust_id>')
def view_beneficiaries(trust_id):
    cursor = get_db_connection()
    trust = cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,)).fetchone()
    beneficiaries = cursor.execute('SELECT * FROM Beneficiaries WHERE TrustID = ?', (trust_id,)).fetchall()
    return render_template('view_beneficiaries.html', trust=trust, beneficiaries=beneficiaries)


@app.route('/add_hgh', methods=('GET', 'POST'))
def add_hgh():
    if request.method == 'POST':
        section_identifier = request.form['section_identifier']
        header_type = request.form['header_type']
        message_create_date = request.form['message_create_date']
        file_layout_version = request.form['file_layout_version']
        unique_file_id = request.form['unique_file_id']
        sars_request_reference = request.form['sars_request_reference']
        test_data_indicator = request.form['test_data_indicator']
        data_type_supplied = request.form['data_type_supplied']
        channel_identifier = request.form['channel_identifier']
        source_identifier = request.form['source_identifier']
        source_system = request.form['source_system']
        source_system_version = request.form['source_system_version']
        contact_person_name = request.form['contact_person_name']
        contact_person_surname = request.form['contact_person_surname']
        business_telephone_number1 = request.form['business_telephone_number1']
        business_telephone_number2 = request.form['business_telephone_number2']
        cell_phone_number = request.form['cell_phone_number']
        contact_email = request.form['contact_email']

        cursor = get_db_connection()

        cursor.execute("""
            INSERT INTO HGHHeaders (SectionIdentifier, HeaderType, MessageCreateDate, FileLayoutVersion, UniqueFileID, SARSRequestReference, TestDataIndicator, DataTypeSupplied, ChannelIdentifier, SourceIdentifier, SourceSystem, SourceSystemVersion, ContactPersonName, ContactPersonSurname, BusinessTelephoneNumber1, BusinessTelephoneNumber2, CellPhoneNumber, ContactEmail)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (section_identifier, header_type, message_create_date, file_layout_version, unique_file_id,
              sars_request_reference, test_data_indicator, data_type_supplied, channel_identifier, source_identifier,
              source_system, source_system_version, contact_person_name, contact_person_surname,
              business_telephone_number1, business_telephone_number2, cell_phone_number, contact_email))
        cursor.commit()
        flash('HGH added!')
        return redirect(url_for('trust_list'))
    current_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    unique_file_id = str(uuid.uuid4())
    return render_template('hgh_list.html', current_datetime=current_datetime, unique_file_id=unique_file_id)


@app.route('/hgh_list')
def hgh_list():
    cursor = get_db_connection()
    cursor.execute('SELECT * FROM HGHHeaders')
    hghs = dictfetchall(cursor)
    return render_template('hgh_list.html', hghs=hghs)


@app.route('/edit_hgh/<int:hgh_id>', methods=('GET', 'POST'))
def edit_hgh(hgh_id):
    cursor = get_db_connection()
    cursor.execute('SELECT * FROM HGHHeaders WHERE ID = ?', (hgh_id,))
    hgh = dictfetchone(cursor)
    if request.method == 'POST':
        section_identifier = request.form['section_identifier']
        header_type = request.form['header_type']
        message_create_date = request.form['message_create_date']
        file_layout_version = request.form['file_layout_version']
        unique_file_id = request.form['unique_file_id']
        sars_request_reference = request.form['sars_request_reference']
        test_data_indicator = request.form['test_data_indicator']
        data_type_supplied = request.form['data_type_supplied']
        channel_identifier = request.form['channel_identifier']
        source_identifier = request.form['source_identifier']
        source_system = request.form['source_system']
        source_system_version = request.form['source_system_version']
        contact_person_name = request.form['contact_person_name']
        contact_person_surname = request.form['contact_person_surname']
        business_telephone_number1 = request.form['business_telephone_number1']
        business_telephone_number2 = request.form['business_telephone_number2']
        cell_phone_number = request.form['cell_phone_number']
        contact_email = request.form['contact_email']

        cursor.execute("""
            UPDATE HGHHeaders SET SectionIdentifier = ?, HeaderType = ?, MessageCreateDate = ?, FileLayoutVersion = ?, UniqueFileID = ?, SARSRequestReference = ?, TestDataIndicator = ?, DataTypeSupplied = ?, ChannelIdentifier = ?, SourceIdentifier = ?, SourceSystem = ?, SourceSystemVersion = ?, ContactPersonName = ?, ContactPersonSurname = ?, BusinessTelephoneNumber1 = ?, BusinessTelephoneNumber2 = ?, CellPhoneNumber = ?, ContactEmail = ?
            WHERE ID = ?
        """, (section_identifier, header_type, message_create_date, file_layout_version, unique_file_id,
              sars_request_reference, test_data_indicator, data_type_supplied, channel_identifier, source_identifier,
              source_system, source_system_version, contact_person_name, contact_person_surname,
              business_telephone_number1, business_telephone_number2, cell_phone_number, contact_email, hgh_id))
        cursor.commit()
        flash('HGH updated!')
        return redirect(url_for('hgh_list'))
    current_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return render_template('hgh_form.html', hgh=hgh, current_datetime=current_datetime)


@app.route('/delete_hgh/<int:hgh_id>')
def delete_hgh(hgh_id):
    cursor = get_db_connection()
    cursor.execute('DELETE FROM HGHHeaders WHERE ID = ?', (hgh_id,))
    cursor.commit()
    flash('HGH deleted!')
    return redirect(url_for('hgh_list'))


@app.route('/add_trust', methods=('GET', 'POST'))
def add_trust():
    if request.method == 'POST':
        trust_reg_number = request.form['TrustRegNumber']
        trust_name = request.form['TrustName']
        tax_number = request.form['TaxNumber']
        nature_of_person = request.form['NatureOfPerson']
        trust_type = request.form['TrustType']
        residency = request.form['Residency']
        masters_office = request.form['MastersOffice']
        physical_unit_number = request.form['PhysicalUnitNumber']
        physical_complex = request.form['PhysicalComplex']
        physical_street_number = request.form['PhysicalStreetNumber']
        physical_street = request.form['PhysicalStreet']
        physical_suburb = request.form['PhysicalSuburb']
        physical_city = request.form['PhysicalCity']
        physical_postal_code = request.form['PhysicalPostalCode']
        postal_same_as_physical = 'PostalSameAsPhysical' in request.form
        postal_address_line1 = request.form['PostalAddressLine1']
        postal_address_line2 = request.form['PostalAddressLine2']
        postal_address_line3 = request.form['PostalAddressLine3']
        postal_address_line4 = request.form['PostalAddressLine4']
        postal_code = request.form['PostalCode']
        contact_number = request.form['ContactNumber']
        cell_number = request.form['CellNumber']
        email = request.form['Email']
        submission_tax_year = request.form.get('SubmissionTaxYear')
        period_start_date = request.form.get('PeriodStartDate')
        period_end_date = request.form.get('PeriodEndDate')
        unique_file_id = str(uuid.uuid4())

        if not trust_reg_number or not trust_name:
            flash('Trust Registration Number and Name are required!')
            return render_template('add_trust.html')
        if tax_number and not modulus_10_check(tax_number):
            flash('Invalid Tax Number!')
            return render_template('add_trust.html')

        cursor = get_db_connection()
        try:
            print('got to the update')
            cursor.execute("""
                INSERT INTO Trusts (TrustRegNumber, TrustName, TaxNumber, NatureOfPerson, TrustType, Residency, MastersOffice,
                PhysicalUnitNumber, PhysicalComplex, PhysicalStreetNumber, PhysicalStreet, PhysicalSuburb, PhysicalCity, PhysicalPostalCode,
                PostalSameAsPhysical, PostalAddressLine1, PostalAddressLine2, PostalAddressLine3, PostalAddressLine4, PostalCode,
                ContactNumber, CellNumber, Email, SubmissionTaxYear, PeriodStartDate, PeriodEndDate, UniqueFileID, RecordStatus)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (trust_reg_number, trust_name, tax_number, nature_of_person, trust_type, residency, masters_office,
                  physical_unit_number, physical_complex, physical_street_number, physical_street, physical_suburb,
                  physical_city, physical_postal_code,
                  postal_same_as_physical, postal_address_line1, postal_address_line2, postal_address_line3,
                  postal_address_line4, postal_code,
                  contact_number, cell_number, email, submission_tax_year, period_start_date, period_end_date,
                  unique_file_id, '0001 - Captured'))
            cursor.commit()

            print('got to the update - committed')



        except Exception:
            flash('Trust registration number must be unique.')
            return render_template('add_trust.html')
        finally:
            print('fianlly')
        return redirect(url_for('trust_list'))
    return render_template('trust_list.html')


@app.route('/edit_trust/<int:trust_id>', methods=('GET', 'POST'))
def edit_trust(trust_id):
    conn = get_db_connection()
    trust = conn.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,)).fetchone()
    if trust is None:
        return 'Trust not found', 404
    if request.method == 'POST':
        trust_reg_number = request.form['TrustRegNumber']
        trust_name = request.form['TrustName']
        tax_number = request.form['TaxNumber']
        nature_of_person = request.form['NatureOfPerson']
        trust_type = request.form['TrustType']
        residency = request.form['Residency']
        masters_office = request.form['MastersOffice']
        date_registered_masters_office = request.form.get('DateRegisteredMastersOffice')
        physical_unit_number = request.form['PhysicalUnitNumber']
        physical_complex = request.form['PhysicalComplex']
        physical_street_number = request.form['PhysicalStreetNumber']
        physical_street = request.form['PhysicalStreet']
        physical_suburb = request.form['PhysicalSuburb']
        physical_city = request.form['PhysicalCity']
        physical_postal_code = request.form['PhysicalPostalCode']
        postal_same_as_physical = 'PostalSameAsPhysical' in request.form
        postal_address_line1 = request.form['PostalAddressLine1']
        postal_address_line2 = request.form['PostalAddressLine2']
        postal_address_line3 = request.form['PostalAddressLine3']
        postal_address_line4 = request.form['PostalAddressLine4']
        postal_code = request.form['PostalCode']
        contact_number = request.form['ContactNumber']
        cell_number = request.form['CellNumber']
        email = request.form['Email']
        submission_tax_year = request.form.get('SubmissionTaxYear')
        period_start_date = request.form.get('PeriodStartDate')
        period_end_date = request.form.get('PeriodEndDate')
        unique_file_id = request.form.get('UniqueFileID', str(uuid.uuid4()))

        if not trust_reg_number or not trust_name:
            flash('Trust Registration Number and Name are required!')
            return render_template('edit_trust.html', trust=trust)
        if tax_number and not modulus_10_check(tax_number):
            flash('Invalid Tax Number!')
            return render_template('edit_trust.html', trust=trust)

        conn.execute("""
            UPDATE Trusts SET TrustRegNumber = ?, TrustName = ?, TaxNumber = ?, NatureOfPerson = ?, TrustType = ?, Residency = ?, MastersOffice = ?, DateRegisteredMastersOffice = ?,
            PhysicalUnitNumber = ?, PhysicalComplex = ?, PhysicalStreetNumber = ?, PhysicalStreet = ?, PhysicalSuburb = ?, PhysicalCity = ?, PhysicalPostalCode = ?,
            PostalSameAsPhysical = ?, PostalAddressLine1 = ?, PostalAddressLine2 = ?, PostalAddressLine3 = ?, PostalAddressLine4 = ?, PostalCode = ?,
            ContactNumber = ?, CellNumber = ?, Email = ?, SubmissionTaxYear = ?, PeriodStartDate = ?, PeriodEndDate = ?, UniqueFileID = ?, RecordStatus = ?
            WHERE TrustID = ?
        """, (trust_reg_number, trust_name, tax_number, nature_of_person, trust_type, residency, masters_office,
              date_registered_masters_office,
              physical_unit_number, physical_complex, physical_street_number, physical_street, physical_suburb,
              physical_city, physical_postal_code,
              postal_same_as_physical, postal_address_line1, postal_address_line2, postal_address_line3,
              postal_address_line4, postal_code,
              contact_number, cell_number, email, submission_tax_year, period_start_date, period_end_date,
              unique_file_id, '0002 - Edited', trust_id))
        conn.commit()
        return redirect(url_for('trusts_list'))
    return render_template('edit_trust.html', trust=trust, mode='capture')


@app.route('/delete_trust/<int:trust_id>')
def delete_trust(trust_id):
    cursor = get_db_connection()
    cursor.execute('DELETE FROM Trusts WHERE TrustID = ?', (trust_id,))
    cursor.commit()
    return redirect(url_for('trust_list'))


@app.route('/add_submission/<int:trust_id>', methods=('GET', 'POST'))
def add_submission(trust_id):
    cursor = get_db_connection()
    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = dictfetchone(cursor)
    if trust is None:
        return 'Trust not found', 404
    if request.method == 'POST':
        submission_date = request.form['SubmissionDate']
        submission_type = request.form['SubmissionType']
        status = request.form['Status']
        software_name = request.form['SoftwareName']
        software_version = request.form['SoftwareVersion']
        user_first_name = request.form['UserFirstName']
        user_last_name = request.form['UserLastName']
        user_contact_number = request.form['UserContactNumber']
        user_email = request.form['UserEmail']
        security_token = str(uuid.uuid4())

        if not user_first_name or not user_last_name:
            flash('User First Name and Last Name are required!')
            return render_template('add_submission.html', trust=trust)

        try:
            cursor.execute("""
                INSERT INTO Submissions (TrustID, SubmissionDate, SubmissionType, Status, SoftwareName, SoftwareVersion,
                UserFirstName, UserLastName, UserContactNumber, UserEmail, SecurityToken)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (trust_id, submission_date, submission_type, status, software_name, software_version,
                  user_first_name, user_last_name, user_contact_number, user_email, security_token))
            cursor.commit()
        except Exception as e:
            flash(f'Error adding submission: {str(e)}')
            return render_template('add_submission.html', trust=trust)
        finally:
            print('fianlly')
        return redirect(url_for('trust_list'))
    return render_template('add_submission.html', trust=trust)


@app.route('/trusts')
def trusts_list():
    mode = request.args.get('mode')
    mode_out = mode
    conn = get_db_connection()

    if mode == 'submissions':
        trusts = conn.execute("SELECT * FROM Trusts ORDER BY RecordStatus DESC")
        mode_out = 'submissions'
    else:
        trusts = conn.execute(
            "SELECT * FROM Trusts WHERE RecordStatus NOT IN ('9010 - SUBMITTED TO SARS','9010 - SUBMITTED TO SARS') ORDER BY RecordStatus DESC")
        mode_out = 'capture'

    return render_template('trusts.html', trusts=trusts, mode=mode_out)


@app.route('/add_beneficiary/<int:trust_id>', methods=('GET', 'POST'))
def add_beneficiary(trust_id):
    conn = get_db_connection()  # <-- this is a Connection
    cur = conn.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = dictfetchone(cur)  # <-- pass a Cursor into dictfetchone
    if trust is None:
        return 'Trust not found', 404

    if request.method == 'POST':

        print()

        nature_of_person = request.form.get('natureOfPerson') or '1'


        print("about to insert beneficiary - ", request.form.get('firstName'))
        cur.execute("""
            INSERT INTO Beneficiaries (TrustID, LastName, FirstName, NatureOfPerson)
            VALUES (?, ?, ?, ?)
        """, (trust_id, request.form.get('last_name'), request.form.get('first_name'), nature_of_person))
        cur.commit()
    return render_template('add_beneficiary.html', trust=trust)



@app.route('/edit_beneficiary/<int:beneficiary_id>', methods=('GET', 'POST'))
def edit_beneficiary(beneficiary_id):
    print(f"edit_beneficiary called for BeneficiaryID={beneficiary_id}")
    conn = get_db_connection()
    cursor = conn.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    beneficiary = dictfetchone(cursor)
    print("Fetched beneficiary:", beneficiary)
    if beneficiary is None:
        print("Beneficiary not found")
        return 'Beneficiary not found', 404

    trust_id = beneficiary['TrustID']
    cursor = conn.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = cursor.fetchone()
    print("Fetched trust:", trust)
    if trust is None:
        print("Trust not found")
        return 'Trust not found', 404

    cursor = conn.cursor()
    cursor.execute('SELECT * FROM BeneficiaryTAD WHERE BeneficiaryID = ?', (beneficiary_id,))
    tad_records = cursor.fetchall()
    print("Fetched TAD records:", tad_records)

    # DNT
    cur_dnt = conn.execute('SELECT * FROM BeneficiaryDNT WHERE BeneficiaryID = ?', (beneficiary_id,))
    dnt_data = dictfetchone(cur_dnt) or {
        'LocalDividends': 0.00,
        'ExemptForeignDividends': 0.00,
        'OtherNonTaxableIncome': 0.00
    }
    print("Fetched DNT data:", dnt_data)

    # TFF
    cur_tff = conn.execute('SELECT * FROM BeneficiaryTFF WHERE BeneficiaryID = ?', (beneficiary_id,))
    tff_data = dictfetchone(cur_tff)  # may be None if no row
    print("Fetched TFF row:", tff_data)

    if request.method == 'POST':
        print("POST received:", request.form)
        print('isNaturalPerson =', request.form.get('isNaturalPerson'))


        date_of_birth = request.form.get('dateOfBirth')
        identification_type = request.form.get('identificationType', '').strip().title()
        id_number = request.form.get('idNumber')

        is_natural_person = 'IsNaturalPerson' in request.form


        converted_type = convert_identification_type(identification_type)
        print("Converted identification type:", converted_type)
        if not converted_type:
            flash('Invalid IdentificationType. Use "South African ID" or "Passport"')
            print("Invalid identification type")
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_data, dnt_data=dnt_data)


        cursor.execute("UPDATE Beneficiaries SET IdentificationType = ? WHERE BeneficiaryID = ?",
                       (converted_type, beneficiary_id))
        print("Updated identification type in DB")

        if identification_type == 'South African ID' and id_number and not sa_id_check(id_number, date_of_birth):
            flash('Invalid South African ID Number or Date of Birth mismatch!')
            print("ID check failed")
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_data, dnt_data=dnt_data)

        if not request.form.get('lastName') or not request.form.get('firstName'):
            flash('Last Name and First Name are required!')
            print("Name missing")
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_data, dnt_data=dnt_data)

        if id_number and not sa_id_check(id_number, date_of_birth):
            flash('Invalid ID Number or Date of Birth mismatch!')
            print("ID check failed (again)")
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_data, dnt_data=dnt_data)

        try:
            print("Beginning transaction")

            cursor2 = conn.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
            print("Beneficiary as per the db:", dictfetchone(cursor2))

            cursor2 = conn.execute('SELECT * FROM BeneficiaryTAD WHERE BeneficiaryID = ?', (beneficiary_id,))
            rows = cursor2.fetchall()
            print("TAD records as per the db:", rows)

            cursor2 = conn.execute('SELECT * FROM BeneficiaryDNT WHERE BeneficiaryID = ?', (beneficiary_id,))
            print("DNT record as per the db:", dictfetchone(cursor2))

            cursor2 = conn.execute('SELECT * FROM BeneficiaryTFF WHERE BeneficiaryID = ?', (beneficiary_id,))
            print("TFF record as per the db:", dictfetchone(cursor2))



            cursor.execute(
                'UPDATE Beneficiaries SET '
                'TaxReferenceNumber = ?, LastName = ?, FirstName = ?, OtherName = ?, Initials = ?, DateOfBirth = ?, '
                'IdentificationType = ?, IDNumber = ?, PassportNumber = ?, PassportCountry = ?, PassportIssueDate = ?, '
                'CompanyIncomeTaxRefNo = ?, CompanyRegistrationNumber = ?, CompanyRegisteredName = ?, NatureOfPerson = ?, '
                'IsConnectedPerson = ?, IsBeneficiary = ?, IsFounder = ?, IsNaturalPerson = ?, IsDonor = ?, IsNonResident = ?, '
                'IsTaxableOnDistributed = ?, HasNonTaxableAmounts = ?, HasCapitalDistribution = ?, MadeDonations = ?, '
                'MadeContributions = ?, ReceivedDonations = ?, ReceivedContributions = ?, MadeDistributions = ?, '
                'ReceivedRefunds = ?, HasRightOfUse = ?, PhysicalUnitNumber = ?, PhysicalComplex = ?, '
                'PhysicalStreetNumber = ?, PhysicalStreet = ?, PhysicalSuburb = ?, PhysicalCity = ?, PhysicalPostalCode = ?, '
                'PostalSameAsPhysical = ?, PostalAddressLine1 = ?, PostalAddressLine2 = ?, PostalAddressLine3 = ?, '
                'PostalAddressLine4 = ?, PostalCode = ?, ContactNumber = ?, CellNumber = ?, Email = ? '
                'WHERE BeneficiaryID = ?',
                (
                    request.form.get('taxReferenceNumber') or None,
                    request.form.get('lastName'),
                    request.form.get('firstName'),
                    request.form.get('otherName') or None,
                    request.form.get('initials') or None,
                    request.form.get('dateOfBirth') or None,
                    converted_type,
                    request.form.get('idNumber') or None,
                    request.form.get('passportNumber') or None,
                    request.form.get('passportCountry') or None,
                    request.form.get('passportIssueDate') or None,
                    request.form.get('companyIncomeTaxRefNo') or None,
                    request.form.get('companyRegistrationNumber') or None,
                    request.form.get('companyRegisteredName') or None,
                    request.form.get('natureOfPerson') or '1',
                    1 if request.form.get('isConnectedPerson') else 0,
                    1 if request.form.get('isBeneficiary') else 0,
                    1 if request.form.get('isFounder') else 0,

                    1 if request.form.get('isNaturalPerson') else 0,

                    1 if request.form.get('isDonor') else 0,
                    1 if request.form.get('isNonResident') else 0,
                    1 if request.form.get('isTaxableOnDistributed') else 0,
                    1 if request.form.get('hasNonTaxableAmounts') else 0,
                    1 if request.form.get('hasCapitalDistribution') else 0,
                    1 if request.form.get('madeDonations') else 0,
                    1 if request.form.get('madeContributions') else 0,
                    1 if request.form.get('receivedDonations') else 0,
                    1 if request.form.get('receivedContributions') else 0,
                    1 if request.form.get('madeDistributions') else 0,
                    1 if request.form.get('receivedRefunds') else 0,
                    1 if request.form.get('hasRightOfUse') else 0,
                    request.form.get('physicalUnitNumber') or None,
                    request.form.get('physicalComplex') or None,
                    request.form.get('physicalStreetNumber') or None,
                    request.form.get('physicalStreet') or None,
                    request.form.get('physicalSuburb') or None,
                    request.form.get('physicalCity') or None,
                    request.form.get('physicalPostalCode') or None,
                    1 if request.form.get('postalSameAsPhysical') else 0,
                    request.form.get('postalAddressLine1') or None,
                    request.form.get('postalAddressLine2') or None,
                    request.form.get('postalAddressLine3') or None,
                    request.form.get('postalAddressLine4') or None,
                    request.form.get('postalCode') or None,
                    request.form.get('contactNumber') or None,
                    request.form.get('cellNumber') or None,
                    request.form.get('email') or None,
                    beneficiary_id
                )
            )
            print("Beneficiary updated")

            # Process TAD records (upsert strategy)
            cursor.execute('DELETE FROM BeneficiaryTAD WHERE BeneficiaryID = ?', (beneficiary_id,))
            print("Deleted old TAD records")
            source_codes = request.form.getlist('tad_source_code[]')
            amounts = request.form.getlist('tad_amount[]')
            foreign_taxes = request.form.getlist('tad_foreign_tax[]')
            print("TAD form lists:", source_codes, amounts, foreign_taxes)
            if len(source_codes) == len(amounts) == len(foreign_taxes):
                for sc, amt, ft in zip(source_codes, amounts, foreign_taxes):
                    try:
                        amt_val = float(amt) if amt.strip() else 0.0
                        ft_val = float(ft) if ft.strip() else 0.0
                    except ValueError:
                        cursor.rollback()
                        print("TAD value error")
                        flash('Invalid amount or foreign tax value! Please enter valid numeric values.')
                        return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                               tad_records=tad_records, tff_data=tff_data, dnt_data=dnt_data)
                    if amt_val >= 0 and ft_val >= 0:
                        cursor.execute(
                            'INSERT INTO BeneficiaryTAD (SectionIdentifier, RecordType, RecordStatus, UniqueNumber, RowNumber, BeneficiaryID, AmountSubjectToTax, SourceCode, ForeignTaxCredits, TrustID) '
                            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                            ('B', 'TAD', 'N', str(uuid.uuid4()), '', beneficiary_id, amt_val, sc, ft_val,
                             beneficiary['TrustID'])
                        )
                        print(f"Inserted TAD record: {sc}, {amt_val}, {ft_val}")

            # Process DNT records (conditional on HasNonTaxableAmounts)
            cursor.execute('DELETE FROM BeneficiaryDNT WHERE BeneficiaryID = ?', (beneficiary_id,))
            print("Deleted old DNT records")
            if request.form.get('hasNonTaxableAmounts'):
                try:
                    local_dividends = float(request.form.get('local_dividends', 0.00) or 0.0)
                    exempt_foreign_dividends = float(request.form.get('exempt_foreign_dividends', 0.00) or 0.0)
                    other_non_taxable = float(request.form.get('other_non_taxable', 0.00) or 0.0)
                except ValueError:
                    cursor.rollback()
                    print("DNT value error")
                    flash('Invalid non-taxable amount values! Please enter valid numeric values.')
                    return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                           tad_records=tad_records, tff_data=tff_data, dnt_data=dnt_data)
                cursor.execute(
                    'INSERT INTO BeneficiaryDNT (SectionIdentifier, RecordType, RecordStatus, BeneficiaryID, LocalDividends, ExemptForeignDividends, OtherNonTaxableIncome, TrustID) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    ('B', 'DNT', 'N', beneficiary_id, local_dividends, exempt_foreign_dividends, other_non_taxable,
                     beneficiary['TrustID'])
                )
                print(f"Inserted DNT record: {local_dividends}, {exempt_foreign_dividends}, {other_non_taxable}")

            # Process TFF records
            cursor.execute('DELETE FROM BeneficiaryTFF WHERE BeneficiaryID = ?', (beneficiary_id,))
            print("Deleted old TFF records")
            try:
                total_value_of_capital_distributed = float(
                    request.form.get('total_value_of_capital_distributed', 0.00) or 0.0)
                total_expenses_incurred = float(request.form.get('total_expenses_incurred', 0.00) or 0.0)
                total_donations_to_trust = float(request.form.get('donations_made', 0.00) or 0.0)
                total_contributions_to_trust = float(request.form.get('total_value_of_contributions_made', 0.00) or 0.0)
                total_donations_received = float(request.form.get('donations_received', 0.00) or 0.0)
                total_contributions_received = float(request.form.get('contributions_received', 0.00) or 0.0)
                total_distributions_to_trust = float(request.form.get('distributions_made', 0.00) or 0.0)
                total_contributions_refunded = float(request.form.get('refunds_received', 0.00) or 0.0)
            except ValueError:
                cursor.rollback()
                print("TFF value error")
                flash('Invalid TFF values! Please enter valid numeric values.')
                return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                       tad_records=tad_records, tff_data=tff_data, dnt_data=dnt_data)
            cursor.execute(
                'INSERT INTO BeneficiaryTFF (SectionIdentifier, RecordType, RecordStatus, BeneficiaryID, '
                'TotalValueOfCapitalDistributed, TotalExpensesIncurred, TotalDonationsToTrust, TotalContributionsToTrust, '
                'TotalDonationsReceivedFromTrust, TotalContributionsReceivedFromTrust, TotalDistributionsToTrust, TotalContributionsRefundedByTrust, TrustID) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                ('B', 'TFF', 'N', beneficiary_id, total_value_of_capital_distributed, total_expenses_incurred,
                 total_donations_to_trust, total_contributions_to_trust, total_donations_received,
                 total_contributions_received, total_distributions_to_trust, total_contributions_refunded,
                 beneficiary['TrustID'])
            )
            print(f"Inserted TFF record: {total_value_of_capital_distributed}, {total_expenses_incurred}, ...")

            cursor.commit()
            print("Transaction committed")
            flash('Beneficiary updated successfully!', 'success')

            cursor2 = conn.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
            print("Beneficiary after commit:", dictfetchone(cursor2))

            cursor2 = conn.execute('SELECT * FROM BeneficiaryTAD WHERE BeneficiaryID = ?', (beneficiary_id,))
            rows = cursor2.fetchall()
            print("TAD records after commit:", rows)

            cursor2 = conn.execute('SELECT * FROM BeneficiaryDNT WHERE BeneficiaryID = ?', (beneficiary_id,))
            print("DNT record after commit:", dictfetchone(cursor2))

            cursor2 = conn.execute('SELECT * FROM BeneficiaryTFF WHERE BeneficiaryID = ?', (beneficiary_id,))
            print("TFF record after commit:", dictfetchone(cursor2))

            return redirect(url_for('view_beneficiaries', trust_id=beneficiary['TrustID']))
        except Exception as e:
            cursor.rollback()
            print("Exception occurred:", str(e))
            flash(f'Unexpected error: {str(e)}')
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_data, dnt_data=dnt_data)

    print("Rendering template for GET or failed POST")
    return render_template('edit_beneficiary.html', beneficiary=beneficiary, tad_records=tad_records, dnt_data=dnt_data,
                           tff_data=tff_data, trust=trust)



@app.route('/delete_beneficiary/<int:beneficiary_id>')
def delete_beneficiary(beneficiary_id):
    conn = get_db_connection()  # this is a Connection

    # Fetch the TrustID for redirect
    cur = conn.execute(
        'SELECT TrustID FROM Beneficiaries WHERE BeneficiaryID = ?',
        (beneficiary_id,)
    )
    row = cur.fetchone()  # now we're calling fetchone() on a Cursor
    if row is None:
        return 'Beneficiary not found', 404

    trust_id = row[0]  # or: dictfetchone(cur)['TrustID'] if you prefer dicts

    # Delete and commit on the CONNECTION (not the cursor)
    conn.execute('DELETE FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    conn.commit()

    return redirect(url_for('view_beneficiaries', trust_id=trust_id))


@app.route('/generate_report')
def generate_report():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Step 2: Execute SQL query (replace with your specific query)
    query = "SELECT trustid, trustname, recordstatus FROM trusts where recordstatus not in ('0000 - IMPORTED', '0002 - Edited')"  # Placeholder: e.g., "SELECT trust_id, beneficiary_name, transaction_date FROM trusts"
    cursor.execute(query)
    rows = cursor.fetchall()

    # Fetch column names for CSV headers
    columns = [desc[0] for desc in cursor.description]

    # Step 3: Generate CSV report in memory
    output = io.StringIO()
    writer = csv.writer(output)

    # Write headers
    writer.writerow(columns)

    # Write data rows
    writer.writerows(rows)

    # Step 4: Prepare response for download
    response = make_response(output.getvalue())
    response.headers["Content-Disposition"] = "attachment; filename=trust_report.csv"
    response.headers["Content-Type"] = "text/csv"

    return response


@app.route('/add_dnt/<int:beneficiary_id>', methods=('GET', 'POST'))
def add_dnt(beneficiary_id):
    cursor = get_db_connection()
    cursor.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    beneficiary = dictfetchone(cursor)
    if beneficiary is None:
        return 'Beneficiary not found', 404

    cursor.execute('SELECT * FROM BeneficiaryDNT WHERE BeneficiaryID = ?', (beneficiary_id,))
    existing_dnt = dictfetchone(cursor)

    if request.method == 'POST':
        local_dividends = request.form.get('LocalDividends', 0.0)
        exempt_foreign_dividends = request.form.get('ExemptForeignDividends', 0.0)
        other_non_taxable_income = request.form.get('OtherNonTaxableIncome', 0.0)

        if existing_dnt:
            cursor.execute("""
                UPDATE BeneficiaryDNT SET LocalDividends = ?, ExemptForeignDividends = ?, OtherNonTaxableIncome = ?
                WHERE BeneficiaryID = ?
            """, (local_dividends, exempt_foreign_dividends, other_non_taxable_income, beneficiary_id))
        else:
            cursor.execute("""
                INSERT INTO BeneficiaryDNT (BeneficiaryID, LocalDividends, ExemptForeignDividends, OtherNonTaxableIncome)
                VALUES (?, ?, ?, ?)
            """, (beneficiary_id, local_dividends, exempt_foreign_dividends, other_non_taxable_income))
        cursor.commit()
        # return redirect(url_for('view_trust', trust_id=beneficiary['TrustID']))

    return render_template('add_dnt.html', beneficiary=beneficiary, dnt=existing_dnt or {})


@app.route('/add_tad/<int:beneficiary_id>', methods=('GET', 'POST'))
def add_tad(beneficiary_id):
    cursor = get_db_connection()
    cursor.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    beneficiary = dictfetchone(cursor)
    if beneficiary is None:
        return 'Beneficiary not found', 404

    if request.method == 'POST':
        source_code = request.form['SourceCode']
        amount_subject_to_tax = request.form.get('AmountSubjectToTax', 0.0)
        foreign_tax_credits = request.form.get('ForeignTaxCredits', 0.0)

        cursor.execute("""
            INSERT INTO BeneficiaryTAD (BeneficiaryID, SourceCode, AmountSubjectToTax, ForeignTaxCredits)
            VALUES (?, ?, ?, ?)
        """, (beneficiary_id, source_code, amount_subject_to_tax, foreign_tax_credits))
        cursor.commit()

        # return redirect(url_for('view_trust', trust_id=beneficiary['TrustID']))

    return render_template('add_tad.html', beneficiary=beneficiary)


@app.route('/edit_tad/<int:tad_id>', methods=('GET', 'POST'))
def edit_tad(tad_id):
    cursor = get_db_connection()
    cursor.execute('SELECT * FROM BeneficiaryTAD WHERE TADID = ?', (tad_id,))
    tad = dictfetchone(cursor)
    if tad is None:
        return 'TAD record not found', 404

    cursor.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (tad['BeneficiaryID'],))
    beneficiary = dictfetchone(cursor)

    if request.method == 'POST':
        source_code = request.form['SourceCode']
        amount_subject_to_tax = request.form.get('AmountSubjectToTax', 0.0)
        foreign_tax_credits = request.form.get('ForeignTaxCredits', 0.0)

        cursor.execute("""
            UPDATE BeneficiaryTAD SET SourceCode = ?, AmountSubjectToTax = ?, ForeignTaxCredits = ?
            WHERE TADID = ?
        """, (source_code, amount_subject_to_tax, foreign_tax_credits, tad_id))
        cursor.commit()
        # return redirect(url_for('view_trust', trust_id=beneficiary['TrustID']))
    return render_template('edit_tad.html', tad=tad, beneficiary=beneficiary)


@app.route('/delete_tad/<int:tad_id>')
def delete_tad(tad_id):
    cursor = get_db_connection()
    cursor.execute('SELECT BeneficiaryID FROM BeneficiaryTAD WHERE TADID = ?', (tad_id,))
    beneficiary_id_row = cursor.fetchone()
    if beneficiary_id_row is None:
        return 'TAD record not found', 404
    beneficiary_id = beneficiary_id_row[0]

    cursor.execute('DELETE FROM BeneficiaryTAD WHERE TADID = ?', (tad_id,))
    cursor.commit()

    cursor.execute('SELECT TrustID FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    trust_id_row = cursor.fetchone()
    trust_id = trust_id_row[0] if trust_id_row else None
    if trust_id:
        return redirect(url_for('beneficiaries', trust_id=trust_id))
    return 'Trust not found', 404


@app.route('/add_tff/<int:beneficiary_id>', methods=('GET', 'POST'))
def add_tff(beneficiary_id):
    cursor = get_db_connection()
    cursor.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    beneficiary = dictfetchone(cursor)
    if beneficiary is None:
        return 'Beneficiary not found', 404

    cursor.execute('SELECT * FROM BeneficiaryTFF WHERE BeneficiaryID = ?', (beneficiary_id,))
    existing_tff = dictfetchone(cursor)

    if request.method == 'POST':
        total_value_of_capital_distributed = request.form.get('TotalValueOfCapitalDistributed', 0.0)
        total_expenses_incurred = request.form.get('TotalExpensesIncurred', 0.0)
        total_donations_to_trust = request.form.get('TotalDonationsToTrust', 0.0)
        total_contributions_to_trust = request.form.get('TotalContributionsToTrust', 0.0)
        total_donations_received_from_trust = request.form.get('TotalDonationsReceivedFromTrust', 0.0)
        total_contributions_received_from_trust = request.form.get('TotalContributionsReceivedFromTrust', 0.0)
        total_distributions_to_trust = request.form.get('TotalDistributionsToTrust', 0.0)
        total_contributions_refunded_by_trust = request.form.get('TotalContributionsRefundedByTrust', 0.0)

        if existing_tff:
            cursor.execute("""
                UPDATE BeneficiaryTFF SET TotalValueOfCapitalDistributed = ?, TotalExpensesIncurred = ?, TotalDonationsToTrust = ?,
                TotalContributionsToTrust = ?, TotalDonationsReceivedFromTrust = ?, TotalContributionsReceivedFromTrust = ?,
                TotalDistributionsToTrust = ?, TotalContributionsRefundedByTrust = ?
                WHERE BeneficiaryID = ?
            """, (total_value_of_capital_distributed, total_expenses_incurred, total_donations_to_trust,
                  total_contributions_to_trust, total_donations_received_from_trust,
                  total_contributions_received_from_trust,
                  total_distributions_to_trust, total_contributions_refunded_by_trust, beneficiary_id))
            cursor.commit()
        else:
            cursor.execute("""
                INSERT INTO BeneficiaryTFF (BeneficiaryID, TotalValueOfCapitalDistributed, TotalExpensesIncurred, TotalDonationsToTrust,
                TotalContributionsToTrust, TotalDonationsReceivedFromTrust, TotalContributionsReceivedFromTrust,
                TotalDistributionsToTrust, TotalContributionsRefundedByTrust)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (beneficiary_id, total_value_of_capital_distributed, total_expenses_incurred, total_donations_to_trust,
                  total_contributions_to_trust, total_donations_received_from_trust,
                  total_contributions_received_from_trust,
                  total_distributions_to_trust, total_contributions_refunded_by_trust))
            cursor.commit()
        # return redirect(url_for('view_trust', trust_id=beneficiary['TrustID']))

    return render_template('add_tff.html', beneficiary=beneficiary, tff=existing_tff or {})


def generate_file_content(trust_id, gh_unique_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM HGHHeaders ORDER BY ID DESC')
    hgh = dictfetchone(cursor)

    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = dictfetchone(cursor)
    trust = {k: sanitize(v) for k, v in trust.items()}

    if not trust:
        raise ValueError("Trust not found")

    cursor.execute('SELECT * FROM Beneficiaries WHERE TrustID = ?', (trust_id,))
    beneficiaries = dictfetchall(cursor)
    beneficiaries = [{k: sanitize(v) for k, v in b.items()} for b in beneficiaries]

    cursor.execute('SELECT * FROM BeneficiaryTAD WHERE TrustID = ?', (trust_id,))
    tad_records = dictfetchall(cursor)
    tad_records = [{k: sanitize(v) for k, v in b.items()} for b in tad_records]

    print()

    header_date = datetime.now()

    lines = []
    gh_line = f"H|GH|{header_date.strftime('%Y-%m-%dT%H:%M:%S')}|1|{gh_unique_id}||T|I3T|HTTPS|9CD036C9-210F-40C5-91F7-82959AB269C02228AE14-53A7-4AC2-A539-A20D1D5654E6F4A52305-B91E-4435-B25E-46E38A528398D60DAB72-1C44-4490-99AF-FA572D3AFC69|GreatSoft|2024.3.1|Karin|Roux|0123428393|0606868076|0823714777|karin@rosspienaar.co.za"

    lines.append(gh_line)

    print('gh_line = ', gh_line)

    dpt_line = (f"B|DPT|N|{trust['TrustRegNumber']}|{trust['TrustName']}|{trust['TaxNumber'] or ''}|"
                f"{trust['SubmissionTaxYear']}|{trust['PeriodStartDate']}|{trust['PeriodEndDate']}|{trust['NatureOfPerson']}|"
                f"{trust['TrustType']}|{trust['Residency']}|{trust['MastersOffice']}|{trust['DateRegisteredMastersOffice'] or ''}|"
                f"{trust['PhysicalUnitNumber'] or ''}|{trust['PhysicalComplex'] or ''}|{trust['PhysicalStreetNumber'] or ''}|"
                f"{trust['PhysicalStreet'] or ''}|{trust['PhysicalSuburb'] or ''}|{trust['PhysicalCity'] or ''}|"
                f"{trust['PhysicalPostalCode'] or ''}|{trust['PostalSameAsPhysical'] and 'Y' or 'N'}|"
                f"{trust['PostalAddressLine1'] or ''}|{trust['PostalAddressLine2'] or ''}|{trust['PostalAddressLine3'] or ''}|"
                f"{trust['PostalAddressLine4'] or ''}|{trust['PostalCode'] or ''}|{trust['ContactNumber'] or ''}|"
                f"{trust['CellNumber'] or ''}|{trust['Email'] or ''}")
    lines.append(dpt_line)


    dpb_count = len(beneficiaries)
    tad_count = len(tad_records)
    tff_count = len(beneficiaries)  # Assuming one TFF per beneficiary

    sequence_number = 3  # After GH and DPT

    for beneficiary in beneficiaries:
        dpb_unique_id = str(uuid.uuid4())
        dpb_line = (f"B|DPB|N|{dpb_unique_id}|{sequence_number}|{beneficiary['TaxReferenceNumber'] or ''}|"
                    f"{beneficiary['LastName']}|{beneficiary['FirstName']}|{beneficiary['OtherName'] or ''}|"
                    f"{beneficiary['Initials'] or ''}|{beneficiary['DateOfBirth'] or ''}|{beneficiary['IdentificationType'] or '001'}|"
                    f"{beneficiary['IDNumber'] or ''}|{beneficiary['PassportNumber'] or ''}|{beneficiary['PassportCountry'] or ''}|"
                    f"{beneficiary['PassportIssueDate'] or ''}|{beneficiary['CompanyIncomeTaxRefNo'] or ''}|"
                    f"{beneficiary['CompanyRegistrationNumber'] or ''}|{beneficiary['CompanyRegisteredName'] or ''}|"
                    f"{beneficiary['NatureOfPerson']}|{beneficiary['IsConnectedPerson'] and 'Y' or 'N'}|"
                    f"{beneficiary['IsBeneficiary'] and 'Y' or 'N'}|{beneficiary['IsFounder'] and 'Y' or 'N'}|"
                    f"{beneficiary['IsNaturalPerson'] and 'Y' or 'N'}|{beneficiary['IsDonor'] and 'Y' or 'N'}|"
                    f"{beneficiary['IsNonResident'] and 'Y' or 'N'}|{beneficiary['PhysicalUnitNumber'] or ''}|"
                    f"{beneficiary['PhysicalComplex'] or ''}|{beneficiary['PhysicalStreetNumber'] or ''}|"
                    f"{beneficiary['PhysicalStreet'] or ''}|{beneficiary['PhysicalSuburb'] or ''}|"
                    f"{beneficiary['PhysicalCity'] or ''}|{beneficiary['PhysicalPostalCode'] or ''}|"
                    f"{beneficiary['PostalSameAsPhysical'] and 'Y' or 'N'}|{beneficiary['PostalAddressLine1'] or ''}|"
                    f"{beneficiary['PostalAddressLine2'] or ''}|{beneficiary['PostalAddressLine3'] or ''}|"
                    f"{beneficiary['PostalAddressLine4'] or ''}|{beneficiary['PostalCode'] or ''}|"
                    f"{beneficiary['ContactNumber'] or '999999999999999'}|{beneficiary['CellNumber'] or '999999999999999'}|"
                    f"{beneficiary['Email'] or ''}|{beneficiary['IsTaxableOnDistributed'] and 'Y' or 'N'}|"
                    f"{beneficiary['HasNonTaxableAmounts'] and 'Y' or 'N'}|{beneficiary['HasCapitalDistribution'] and 'Y' or 'N'}|"
                    f"{beneficiary['MadeDonations'] and 'Y' or 'N'}|{beneficiary['MadeContributions'] and 'Y' or 'N'}|"
                    f"{beneficiary['ReceivedDonations'] and 'Y' or 'N'}|{beneficiary['ReceivedContributions'] and 'Y' or 'N'}|"
                    f"{beneficiary['MadeDistributions'] and 'Y' or 'N'}|{beneficiary['ReceivedRefunds'] and 'Y' or 'N'}|"
                    f"{beneficiary['HasRightOfUse'] and 'Y' or 'N'}")
        lines.append(dpb_line)
        sequence_number += 1

        cursor.execute('SELECT * FROM BeneficiaryTAD WHERE BeneficiaryID = ?', (beneficiary['BeneficiaryID'],))
        tad_records1 = dictfetchall(cursor)

        for tad in tad_records1:
            amount = '' if float(tad['AmountSubjectToTax']) == 0 else tad['AmountSubjectToTax']
            source_code = '' if float(tad['SourceCode']) == 0 else tad['SourceCode']
            foreign_credits = '' if float(tad['ForeignTaxCredits']) == 0 else tad['ForeignTaxCredits']

            tad_line = (f"{tad['SectionIdentifier']}"
                        f"|{tad['RecordType']}"
                        f"|{tad['RecordStatus']}"
                        f"|{tad['UniqueNumber'] or str(uuid.uuid4())}"
                        f"|{sequence_number}"
                        f"|{dpb_unique_id}"
                        f"|{amount}"
                        f"|{source_code}"
                        f"|{foreign_credits}")
            lines.append(tad_line)
            sequence_number += 1

        cursor.execute('SELECT * FROM BeneficiaryDNT WHERE BeneficiaryID = ?', (beneficiary['BeneficiaryID'],))
        dnt_row = dictfetchone(cursor)

        dnt_data = dnt_row if dnt_row else {
            'LocalDividends': 0.00,
            'ExemptForeignDividends': 0.00,
            'OtherNonTaxableIncome': 0.00
        }

        for key in ['LocalDividends', 'ExemptForeignDividends', 'OtherNonTaxableIncome']:
            try:
                dnt_data[key] = int(float(dnt_data[key]))
            except (ValueError, TypeError):
                dnt_data[key] = 0

        if beneficiary['HasNonTaxableAmounts'] == 1:
            dnt_line = (
                f"B|DNT|N|{str(uuid.uuid4())}|{sequence_number}|{dpb_unique_id}|"
                f"{dnt_data['LocalDividends']}|{dnt_data['ExemptForeignDividends']}|{dnt_data['OtherNonTaxableIncome']}"
            )
            lines.append(dnt_line)
            sequence_number += 1

        cursor.execute('SELECT * FROM BeneficiaryTFF WHERE BeneficiaryID = ?', (beneficiary['BeneficiaryID'],))
        tff_row = dictfetchall(cursor)

        totalValueOfCapitalDistributed = int(sum(float(row['TotalValueOfCapitalDistributed']) for row in tff_row))
        totalExpensesIncurred = int(sum(float(row['TotalExpensesIncurred']) for row in tff_row))
        totalDonationsToTrust = int(sum(float(row['TotalDonationsToTrust']) for row in tff_row))
        totalContributionsToTrust = int(sum(float(row['TotalContributionsToTrust']) for row in tff_row))
        totalDonationsReceivedFromTrust = int(sum(float(row['TotalDonationsReceivedFromTrust']) for row in tff_row))
        totalContributionsReceivedFromTrust = int(
            sum(float(row['TotalContributionsReceivedFromTrust']) for row in tff_row))
        totalDistributionsToTrust = int(sum(float(row['TotalDistributionsToTrust']) for row in tff_row))
        totalContributionsRefundedByTrust = int(sum(float(row['TotalContributionsRefundedByTrust']) for row in tff_row))

        tff_line = (f"B|TFF|N|"
                    f"{str(uuid.uuid4())}|{sequence_number}|{dpb_unique_id}|{totalValueOfCapitalDistributed}|"
                    f"{totalExpensesIncurred}|"
                    f"{totalDonationsToTrust}|"
                    f"{totalContributionsToTrust}|"
                    f"{totalDonationsReceivedFromTrust}|"
                    f"{totalContributionsReceivedFromTrust}|"
                    f"{totalDistributionsToTrust}|"
                    f"{totalContributionsRefundedByTrust}"
                    f"")
        lines.append(tff_line)
        sequence_number += 1

    file_body = ''.join(lines)
    md5_hash = hashlib.md5(file_body.encode('utf-8')).hexdigest()

    cursor.execute('SELECT * FROM BeneficiaryDNT WHERE TrustID = ?', (trust_id,))
    dnt_row = dictfetchall(cursor)

    cursor.execute('SELECT * FROM BeneficiaryTFF WHERE TrustID = ?', (trust_id,))
    tff_row = dictfetchall(cursor)

    totalValueOfCapitalDistributed = int(sum(float(row['TotalValueOfCapitalDistributed']) for row in tff_row))
    totalExpensesIncurred = int(sum(float(row['TotalExpensesIncurred']) for row in tff_row))
    totalDonationsToTrust = int(sum(float(row['TotalDonationsToTrust']) for row in tff_row))
    totalContributionsToTrust = int(sum(float(row['TotalContributionsToTrust']) for row in tff_row))
    totalDonationsReceivedFromTrust = int(sum(float(row['TotalDonationsReceivedFromTrust']) for row in tff_row))
    totalContributionsReceivedFromTrust = int(sum(float(row['TotalContributionsReceivedFromTrust']) for row in tff_row))
    totalDistributionsToTrust = int(sum(float(row['TotalDistributionsToTrust']) for row in tff_row))
    totalContributionsRefundedByTrust = int(sum(float(row['TotalContributionsRefundedByTrust']) for row in tff_row))

    total_amount = int(sum(float(row['AmountSubjectToTax']) for row in tad_records))
    total_amount += int(sum(float(row['ForeignTaxCredits']) for row in tad_records))
    total_amount += int(sum(float(row['LocalDividends']) for row in dnt_row))
    total_amount += int(sum(float(row['ExemptForeignDividends']) for row in dnt_row))
    total_amount += int(sum(float(row['OtherNonTaxableIncome']) for row in dnt_row))
    total_amount += totalValueOfCapitalDistributed
    total_amount += totalExpensesIncurred
    total_amount += totalDonationsToTrust
    total_amount += totalContributionsToTrust
    total_amount += totalDonationsReceivedFromTrust
    total_amount += totalContributionsReceivedFromTrust
    total_amount += totalDistributionsToTrust
    total_amount += totalContributionsRefundedByTrust

    t_line = f"T|{sequence_number - 1}|{md5_hash}|{total_amount:.2f}"
    lines.append(t_line)

    print('lines =', lines)

    timestamp = header_date.strftime('%Y%m%dT%H%M%S')
    filename = f"I3T_1_1517179642_{gh_unique_id}_{timestamp}_B2BSFG.txt"
    return filename, '\n'.join(lines), trust, dpb_count, tad_count, tff_count, total_amount


@app.route('/generate_i3t_direct/<int:trust_id>')
def generate_i3t_direct(trust_id):
    try:
        gh_unique_id = str(uuid.uuid4())
        filename, file_content, trust, dpb_count, tad_count, tff_count, total_amount = generate_file_content(trust_id,
                                                                                                             gh_unique_id)
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE Trusts SET RecordStatus = '9001 - Submitted to SARS' WHERE TrustID = ?", (trust_id,))
        cursor.commit()

        file_buffer = io.StringIO(file_content)
        file_buffer.seek(0)
        return send_file(
            io.BytesIO(file_content.encode('ISO-8859-1')),
            download_name=filename,
            as_attachment=True,
            mimetype='text/plain'
        )
    except ValueError as e:
        return str(e), 404
    except Exception as e:
        return f"Error generating SARS file: {str(e)}", 500


@app.route('/generate_sars_file/<int:trust_id>')
def generate_sars_file(trust_id):
    try:
        gh_unique_id = request.args.get('gh_unique_id')
        if not gh_unique_id:
            flash('Proposed Unique ID is required to generate SARS file!')
            return redirect(url_for('generate_i3t', trust_id=trust_id))

        filename, file_content, trust, _, _, _, _ = generate_file_content(trust_id, gh_unique_id)
        file_buffer = io.StringIO(file_content)
        file_buffer.seek(0)
        return send_file(
            io.BytesIO(file_content.encode('ISO-8859-1')),
            download_name=filename,
            as_attachment=True,
            mimetype='text/plain'
        )
    except Exception as e:
        return f"Error generating SARS file: {str(e)}", 500


def none_to_blank(value):
    return '' if value in (None, 'None', 'null', 'none', '<null>') else value


@app.route('/export_data', methods=['GET'])
def export_data():
    cursor = get_db_connection()
    tables = ['Beneficiaries', 'BeneficiaryDNT', 'BeneficiaryTAD', 'BeneficiaryTFF', 'HGHHeaders', 'Submissions',
              'Trusts']
    data = {}
    for table in tables:
        cursor.execute(f'SELECT * FROM {table}')
        data[table] = dictfetchall(cursor)

    response = app.response_class(
        response=json.dumps(data, default=str),
        status=200,
        mimetype='application/json'
    )
    response.headers['Content-Disposition'] = 'attachment; filename=backup.json'
    return response


@app.route('/import_data', methods=['GET'])
def import_data():
    return render_template('import_data.html')


@app.route('/kill_process_tree')
def kill_process_tree():
    try:
        result = subprocess.run(
            ['taskkill', '/IM', 'RossPienaar_IT3(t).exe', '/T', '/F'],
            capture_output=True,
            text=True,
            check=True
        )
        flash('Process tree killed successfully: ' + result.stdout, 'success')
    except subprocess.CalledProcessError as e:
        flash(f'Failed to kill process: {e.stderr}', 'error')
    except Exception as e:
        flash(f'Error: {str(e)}', 'error')
    return redirect(url_for('index'))


app.jinja_env.filters['none_to_blank'] = none_to_blank

if __name__ == '__main__':
    # threading.Timer(1.25, lambda: webbrowser.open('http://127.0.0.1:5000')).start()
    app.run(debug=True)
