import webbrowser
import threading
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pyodbc
import os
import io
from datetime import datetime
import uuid
import json
import hashlib

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'fallback-secret-key')


def sanitize(value):
    return '' if value in (None, 'None', 'none') else str(value)


from flask import g


def get_db_connection():
    if 'db' not in g:
        conn_str = os.getenv('AZURE_SQL_CONNECTION_STRING')
        if not conn_str:
            raise ValueError("AZURE_SQL_CONNECTION_STRING not set")
        params = dict(p.split('=') for p in conn_str.split(';') if p)
        server = params.get('server')
        database = params.get('database')
        user = params.get('user') or params.get('user id')
        password = params.get('password')
        odbc_conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            "Encrypt=yes;TrustServerCertificate=no;"
        )
        g.db = pyodbc.connect(odbc_conn_str, autocommit=True)
    return g.db


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
    conn = get_db_connection()
    trust = conn.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,)).fetchone()
    beneficiaries = conn.execute('SELECT * FROM Beneficiaries WHERE TrustID = ?', (trust_id,)).fetchall()
    # conn.close()
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

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO HGHHeaders (SectionIdentifier, HeaderType, MessageCreateDate, FileLayoutVersion, UniqueFileID, SARSRequestReference, TestDataIndicator, DataTypeSupplied, ChannelIdentifier, SourceIdentifier, SourceSystem, SourceSystemVersion, ContactPersonName, ContactPersonSurname, BusinessTelephoneNumber1, BusinessTelephoneNumber2, CellPhoneNumber, ContactEmail)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (section_identifier, header_type, message_create_date, file_layout_version, unique_file_id,
              sars_request_reference, test_data_indicator, data_type_supplied, channel_identifier, source_identifier,
              source_system, source_system_version, contact_person_name, contact_person_surname,
              business_telephone_number1, business_telephone_number2, cell_phone_number, contact_email))
        # conn.close()
        flash('HGH added!')
        return redirect(url_for('trusts'))
    current_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    unique_file_id = str(uuid.uuid4())
    return render_template('hgh_list.html', current_datetime=current_datetime, unique_file_id=unique_file_id)


@app.route('/hgh_list')
def hgh_list():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM HGHHeaders')
    hghs = dictfetchall(cursor)
    # conn.close()
    return render_template('hgh_list.html', hghs=hghs)


@app.route('/edit_hgh/<int:hgh_id>', methods=('GET', 'POST'))
def edit_hgh(hgh_id):
    conn = get_db_connection()
    cursor = conn.cursor()
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
        # conn.close()
        flash('HGH updated!')
        return redirect(url_for('hgh_list'))
    # conn.close()
    current_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return render_template('hgh_form.html', hgh=hgh, current_datetime=current_datetime)


@app.route('/delete_hgh/<int:hgh_id>')
def delete_hgh(hgh_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM HGHHeaders WHERE ID = ?', (hgh_id,))
    # conn.close()
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

        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO Trusts (TrustRegNumber, TrustName, TaxNumber, NatureOfPerson, TrustType, Residency, MastersOffice,
                PhysicalUnitNumber, PhysicalComplex, PhysicalStreetNumber, PhysicalStreet, PhysicalSuburb, PhysicalCity, PhysicalPostalCode,
                PostalSameAsPhysical, PostalAddressLine1, PostalAddressLine2, PostalAddressLine3, PostalAddressLine4, PostalCode,
                ContactNumber, CellNumber, Email, SubmissionTaxYear, PeriodStartDate, PeriodEndDate, UniqueFileID)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (trust_reg_number, trust_name, tax_number, nature_of_person, trust_type, residency, masters_office,
                  physical_unit_number, physical_complex, physical_street_number, physical_street, physical_suburb,
                  physical_city, physical_postal_code,
                  postal_same_as_physical, postal_address_line1, postal_address_line2, postal_address_line3,
                  postal_address_line4, postal_code,
                  contact_number, cell_number, email, submission_tax_year, period_start_date, period_end_date,
                  unique_file_id))
        except Exception:
            flash('Trust registration number must be unique.')
            return render_template('add_trust.html')
        finally:
            print('fianlly')
            # conn.close()
        return redirect(url_for('trusts'))
    return render_template('add_trust.html')


@app.route('/edit_trust/<int:trust_id>', methods=('GET', 'POST'))
def edit_trust(trust_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = dictfetchone(cursor)
    if trust is None:
        # conn.close()
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

        cursor.execute("""
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
        # conn.close()
        return redirect(url_for('trusts_list'))
    # conn.close()
    return render_template('edit_trust.html', trust=trust, mode='capture')


@app.route('/delete_trust/<int:trust_id>')
def delete_trust(trust_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM Trusts WHERE TrustID = ?', (trust_id,))
    # conn.close()
    return redirect(url_for('trusts'))


@app.route('/add_submission/<int:trust_id>', methods=('GET', 'POST'))
def add_submission(trust_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = dictfetchone(cursor)
    if trust is None:
        # conn.close()
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
        except Exception as e:
            flash(f'Error adding submission: {str(e)}')
            return render_template('add_submission.html', trust=trust)
        finally:
            print('fianlly')
            # conn.close()
        return redirect(url_for('trusts'))
    # conn.close()
    return render_template('add_submission.html', trust=trust)


@app.route('/trusts')
def trusts_list():
    mode = request.args.get('mode')
    mode_out = mode
    conn = get_db_connection()
    cursor = conn.cursor()

    if mode == 'submissions':
        cursor.execute("SELECT * FROM Trusts WHERE RecordStatus <> '0000 - Imported' ORDER BY RecordStatus DESC")
        mode_out = 'submissions'
    else:
        cursor.execute(
            "SELECT * FROM Trusts WHERE RecordStatus NOT IN ('9010 - SUBMITTED TO SARS') ORDER BY RecordStatus DESC")
        mode_out = 'capture'

    trusts = dictfetchall(cursor)
    # conn.close()
    return render_template('trusts.html', trusts=trusts, mode=mode_out)


@app.route('/view_trust/<int:trust_id>')
def view_trust(trust_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = dictfetchone(cursor)
    if trust is None:
        # conn.close()
        return 'Trust not found', 404

    cursor.execute('SELECT * FROM Beneficiaries WHERE TrustID = ?', (trust_id,))
    beneficiaries = dictfetchall(cursor)

    cursor.execute('SELECT * FROM Submissions WHERE TrustID = ?', (trust_id,))
    submissions = dictfetchall(cursor)
    # conn.close()
    return render_template('view_trust.html', trust=trust, beneficiaries=beneficiaries, submissions=submissions)


@app.route('/add_beneficiary/<int:trust_id>', methods=('GET', 'POST'))
def add_beneficiary(trust_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = dictfetchone(cursor)
    if trust is None:
        # conn.close()
        return 'Trust not found', 404
    if request.method == 'POST':
        tax_reference_number = request.form.get('TaxReferenceNumber')
        last_name = request.form['LastName']
        first_name = request.form['FirstName']
        other_name = request.form.get('OtherName')
        initials = request.form.get('Initials')
        date_of_birth = request.form.get('DateOfBirth')
        id_number = request.form.get('IDNumber')
        identification_type = request.form.get('IdentificationType')
        passport_number = request.form.get('PassportNumber')
        passport_country = request.form.get('PassportCountry')
        passport_issue_date = request.form.get('PassportIssueDate')
        company_registration_number = request.form.get('CompanyRegistrationNumber')
        company_registered_name = request.form.get('CompanyRegisteredName')
        nature_of_person = request.form['NatureOfPerson']
        is_connected_person = 'IsConnectedPerson' in request.form
        is_beneficiary = 'IsBeneficiary' in request.form
        is_founder = 'IsFounder' in request.form
        is_natural_person = 'IsNaturalPerson' in request.form
        is_donor = 'IsDonor' in request.form
        is_non_resident = 'IsNonResident' in request.form
        is_taxable_on_distributed = 'IsTaxableOnDistributed' in request.form
        has_non_taxable_amounts = 'HasNonTaxableAmounts' in request.form
        has_capital_distribution = 'HasCapitalDistribution' in request.form
        has_loans_granted = 'HasLoansGranted' in request.form
        has_loans_from = 'HasLoansFrom' in request.form
        made_donations = 'MadeDonations' in request.form
        made_contributions = 'MadeContributions' in request.form
        received_donations = 'ReceivedDonations' in request.form
        received_contributions = 'ReceivedContributions' in request.form
        made_distributions = 'MadeDistributions' in request.form
        received_refunds = 'ReceivedRefunds' in request.form
        has_right_of_use = 'HasRightOfUse' in request.form
        physical_unit_number = request.form.get('PhysicalUnitNumber')
        physical_complex = request.form.get('PhysicalComplex')
        physical_street_number = request.form.get('PhysicalStreetNumber')
        physical_street = request.form.get('PhysicalStreet')
        physical_suburb = request.form.get('PhysicalSuburb')
        physical_city = request.form.get('PhysicalCity')
        physical_postal_code = request.form.get('PhysicalPostalCode')
        postal_same_as_physical = 'PostalSameAsPhysical' in request.form
        postal_address_line1 = request.form.get('PostalAddressLine1')
        postal_address_line2 = request.form.get('PostalAddressLine2')
        postal_address_line3 = request.form.get('PostalAddressLine3')
        postal_address_line4 = request.form.get('PostalAddressLine4')
        postal_code = request.form.get('PostalCode')
        contact_number = request.form.get('ContactNumber')
        cell_number = request.form.get('CellNumber')
        email = request.form.get('Email')
        company_income_tax_ref_no = request.form.get('CompanyIncomeTaxRefNo')

        if not last_name or not first_name:
            flash('Last Name and First Name are required!')
            return render_template('add_beneficiary.html', trust=trust)

        if id_number and not sa_id_check(id_number, date_of_birth):
            flash('Invalid ID Number or Date of Birth mismatch!')
            return render_template('add_beneficiary.html', trust=trust)

        cursor.execute("""
            INSERT INTO Beneficiaries (TrustID, TaxReferenceNumber, LastName, FirstName, OtherName, Initials, DateOfBirth,
            IDNumber, IdentificationType, PassportNumber, PassportCountry, PassportIssueDate, CompanyRegistrationNumber,
            CompanyRegisteredName, NatureOfPerson, IsConnectedPerson, IsBeneficiary, IsFounder, IsNaturalPerson, IsDonor,
            IsNonResident, IsTaxableOnDistributed, HasNonTaxableAmounts, HasCapitalDistribution, HasLoansGranted, HasLoansFrom,
            MadeDonations, MadeContributions, ReceivedDonations, ReceivedContributions, MadeDistributions, ReceivedRefunds,
            HasRightOfUse, PhysicalUnitNumber, PhysicalComplex, PhysicalStreetNumber, PhysicalStreet, PhysicalSuburb,
            PhysicalCity, PhysicalPostalCode, PostalSameAsPhysical, PostalAddressLine1, PostalAddressLine2, PostalAddressLine3,
            PostalAddressLine4, PostalCode, ContactNumber, CellNumber, Email, CompanyIncomeTaxRefNo)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (trust_id, tax_reference_number, last_name, first_name, other_name, initials, date_of_birth,
              id_number, identification_type, passport_number, passport_country, passport_issue_date,
              company_registration_number, company_registered_name, nature_of_person, is_connected_person,
              is_beneficiary, is_founder, is_natural_person, is_donor, is_non_resident, is_taxable_on_distributed,
              has_non_taxable_amounts, has_capital_distribution, has_loans_granted, has_loans_from, made_donations,
              made_contributions, received_donations, received_contributions, made_distributions, received_refunds,
              has_right_of_use, physical_unit_number, physical_complex, physical_street_number, physical_street,
              physical_suburb, physical_city, physical_postal_code, postal_same_as_physical, postal_address_line1,
              postal_address_line2, postal_address_line3, postal_address_line4, postal_code, contact_number,
              cell_number, email, company_income_tax_ref_no))
        # conn.close()
        return redirect(url_for('view_trust', trust_id=trust_id))
    # conn.close()
    return render_template('add_beneficiary.html', trust=trust)


@app.route('/edit_beneficiary/<int:beneficiary_id>', methods=('GET', 'POST'))
def edit_beneficiary(beneficiary_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    beneficiary = dictfetchone(cursor)
    if beneficiary is None:
        # conn.close()
        return 'Beneficiary not found', 404

    trust_id = beneficiary['TrustID']
    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = dictfetchone(cursor)

    # Fetch TAD records
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM BeneficiaryTAD WHERE BeneficiaryID = ?', (beneficiary_id,))
    tad_records = cursor.fetchall()

    dnt_row = conn.execute('SELECT * FROM BeneficiaryDNT WHERE BeneficiaryID = ?', (beneficiary_id,)).fetchone()
    dnt_data = dict(dnt_row) if dnt_row else {
        'LocalDividends': 0.00,
        'ExemptForeignDividends': 0.00,
        'OtherNonTaxableIncome': 0.00
    }

    tff_row = conn.execute('SELECT * FROM BeneficiaryTFF WHERE BeneficiaryID = ?', (beneficiary_id,)).fetchone()

    if request.method == 'POST':
        tax_reference_number = request.form.get('TaxReferenceNumber')
        last_name = request.form['LastName']
        first_name = request.form['FirstName']
        other_name = request.form.get('OtherName')
        initials = request.form.get('Initials')
        date_of_birth = request.form.get('DateOfBirth')
        id_number = request.form.get('IDNumber')
        identification_type = request.form.get('IdentificationType')
        passport_number = request.form.get('PassportNumber')
        passport_country = request.form.get('PassportCountry')
        passport_issue_date = request.form.get('PassportIssueDate')
        company_registration_number = request.form.get('CompanyRegistrationNumber')
        company_registered_name = request.form.get('CompanyRegisteredName')
        nature_of_person = request.form['NatureOfPerson']
        is_connected_person = 'IsConnectedPerson' in request.form
        is_beneficiary = 'IsBeneficiary' in request.form
        is_founder = 'IsFounder' in request.form
        is_natural_person = 'IsNaturalPerson' in request.form
        is_donor = 'IsDonor' in request.form
        is_non_resident = 'IsNonResident' in request.form
        is_taxable_on_distributed = 'IsTaxableOnDistributed' in request.form
        has_non_taxable_amounts = 'HasNonTaxableAmounts' in request.form
        has_capital_distribution = 'HasCapitalDistribution' in request.form
        has_loans_granted = 'HasLoansGranted' in request.form
        has_loans_from = 'HasLoansFrom' in request.form
        made_donations = 'MadeDonations' in request.form
        made_contributions = 'MadeContributions' in request.form
        received_donations = 'ReceivedDonations' in request.form
        received_contributions = 'ReceivedContributions' in request.form
        made_distributions = 'MadeDistributions' in request.form
        received_refunds = 'ReceivedRefunds' in request.form
        has_right_of_use = 'HasRightOfUse' in request.form
        physical_unit_number = request.form.get('PhysicalUnitNumber')
        physical_complex = request.form.get('PhysicalComplex')
        physical_street_number = request.form.get('PhysicalStreetNumber')
        physical_street = request.form.get('PhysicalStreet')
        physical_suburb = request.form.get('PhysicalSuburb')
        physical_city = request.form.get('PhysicalCity')
        physical_postal_code = request.form.get('PhysicalPostalCode')
        postal_same_as_physical = 'PostalSameAsPhysical' in request.form
        postal_address_line1 = request.form.get('PostalAddressLine1')
        postal_address_line2 = request.form.get('PostalAddressLine2')
        postal_address_line3 = request.form.get('PostalAddressLine3')
        postal_address_line4 = request.form.get('PostalAddressLine4')
        postal_code = request.form.get('PostalCode')
        contact_number = request.form.get('ContactNumber')
        cell_number = request.form.get('CellNumber')
        email = request.form.get('Email')
        company_income_tax_ref_no = request.form.get('CompanyIncomeTaxRefNo')

        if not last_name or not first_name:
            flash('Last Name and First Name are required!')
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_row)

        if id_number and not sa_id_check(id_number, date_of_birth):
            flash('Invalid ID Number or Date of Birth mismatch!')
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_row)

        cursor.execute("""
            UPDATE Beneficiaries SET TaxReferenceNumber = ?, LastName = ?, FirstName = ?, OtherName = ?, Initials = ?, DateOfBirth = ?,
            IDNumber = ?, IdentificationType = ?, PassportNumber = ?, PassportCountry = ?, PassportIssueDate = ?, CompanyRegistrationNumber = ?,
            CompanyRegisteredName = ?, NatureOfPerson = ?, IsConnectedPerson = ?, IsBeneficiary = ?, IsFounder = ?, IsNaturalPerson = ?, IsDonor = ?,
            IsNonResident = ?, IsTaxableOnDistributed = ?, HasNonTaxableAmounts = ?, HasCapitalDistribution = ?, HasLoansGranted = ?, HasLoansFrom = ?,
            MadeDonations = ?, MadeContributions = ?, ReceivedDonations = ?, ReceivedContributions = ?, MadeDistributions = ?, ReceivedRefunds = ?,
            HasRightOfUse = ?, PhysicalUnitNumber = ?, PhysicalComplex = ?, PhysicalStreetNumber = ?, PhysicalStreet = ?, PhysicalSuburb = ?,
            PhysicalCity = ?, PhysicalPostalCode = ?, PostalSameAsPhysical = ?, PostalAddressLine1 = ?, PostalAddressLine2 = ?, PostalAddressLine3 = ?,
            PostalAddressLine4 = ?, PostalCode = ?, ContactNumber = ?, CellNumber = ?, Email = ?, CompanyIncomeTaxRefNo = ?
            WHERE BeneficiaryID = ?
        """, (tax_reference_number, last_name, first_name, other_name, initials, date_of_birth,
              id_number, identification_type, passport_number, passport_country, passport_issue_date,
              company_registration_number, company_registered_name, nature_of_person, is_connected_person,
              is_beneficiary, is_founder, is_natural_person, is_donor, is_non_resident, is_taxable_on_distributed,
              has_non_taxable_amounts, has_capital_distribution, has_loans_granted, has_loans_from, made_donations,
              made_contributions, received_donations, received_contributions, made_distributions, received_refunds,
              has_right_of_use, physical_unit_number, physical_complex, physical_street_number, physical_street,
              physical_suburb, physical_city, physical_postal_code, postal_same_as_physical, postal_address_line1,
              postal_address_line2, postal_address_line3, postal_address_line4, postal_code, contact_number,
              cell_number, email, company_income_tax_ref_no, beneficiary_id))
        # conn.close()
        return redirect(url_for('view_trust', trust_id=trust_id))
    # conn.close()
    return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust, tad_records=tad_records, tff_data=tff_row, dnt_data=dnt_data)


@app.route('/delete_beneficiary/<int:beneficiary_id>')
def delete_beneficiary(beneficiary_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT TrustID FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    trust_id_row = cursor.fetchone()
    if trust_id_row is None:
        # conn.close()
        return 'Beneficiary not found', 404
    trust_id = trust_id_row[0]
    cursor.execute('DELETE FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    # conn.close()
    return redirect(url_for('view_trust', trust_id=trust_id))


@app.route('/add_dnt/<int:beneficiary_id>', methods=('GET', 'POST'))
def add_dnt(beneficiary_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    beneficiary = dictfetchone(cursor)
    if beneficiary is None:
        # conn.close()
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
        # conn.close()
        return redirect(url_for('view_trust', trust_id=beneficiary['TrustID']))
    # conn.close()
    return render_template('add_dnt.html', beneficiary=beneficiary, dnt=existing_dnt or {})


@app.route('/add_tad/<int:beneficiary_id>', methods=('GET', 'POST'))
def add_tad(beneficiary_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    beneficiary = dictfetchone(cursor)
    if beneficiary is None:
        # conn.close()
        return 'Beneficiary not found', 404

    if request.method == 'POST':
        source_code = request.form['SourceCode']
        amount_subject_to_tax = request.form.get('AmountSubjectToTax', 0.0)
        foreign_tax_credits = request.form.get('ForeignTaxCredits', 0.0)

        cursor.execute("""
            INSERT INTO BeneficiaryTAD (BeneficiaryID, SourceCode, AmountSubjectToTax, ForeignTaxCredits)
            VALUES (?, ?, ?, ?)
        """, (beneficiary_id, source_code, amount_subject_to_tax, foreign_tax_credits))
        # conn.close()
        return redirect(url_for('view_trust', trust_id=beneficiary['TrustID']))
    # conn.close()
    return render_template('add_tad.html', beneficiary=beneficiary)


@app.route('/edit_tad/<int:tad_id>', methods=('GET', 'POST'))
def edit_tad(tad_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM BeneficiaryTAD WHERE TADID = ?', (tad_id,))
    tad = dictfetchone(cursor)
    if tad is None:
        # conn.close()
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
        # conn.close()
        return redirect(url_for('view_trust', trust_id=beneficiary['TrustID']))
    # conn.close()
    return render_template('edit_tad.html', tad=tad, beneficiary=beneficiary)


@app.route('/delete_tad/<int:tad_id>')
def delete_tad(tad_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT BeneficiaryID FROM BeneficiaryTAD WHERE TADID = ?', (tad_id,))
    beneficiary_id_row = cursor.fetchone()
    if beneficiary_id_row is None:
        # conn.close()
        return 'TAD record not found', 404
    beneficiary_id = beneficiary_id_row[0]

    cursor.execute('DELETE FROM BeneficiaryTAD WHERE TADID = ?', (tad_id,))

    cursor.execute('SELECT TrustID FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    trust_id_row = cursor.fetchone()
    trust_id = trust_id_row[0] if trust_id_row else None
    # conn.close()
    if trust_id:
        return redirect(url_for('view_trust', trust_id=trust_id))
    return 'Trust not found', 404


@app.route('/add_tff/<int:beneficiary_id>', methods=('GET', 'POST'))
def add_tff(beneficiary_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    beneficiary = dictfetchone(cursor)
    if beneficiary is None:
        # conn.close()
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
        # conn.close()
        return redirect(url_for('view_trust', trust_id=beneficiary['TrustID']))
    # conn.close()
    return render_template('add_tff.html', beneficiary=beneficiary, tff=existing_tff or {})


def generate_file_content(trust_id, gh_unique_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM HGHHeaders ORDER BY ID DESC')
    hgh = dictfetchone(cursor)
    if not hgh:
        raise ValueError("No HGH header found")

    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = dictfetchone(cursor)
    if not trust:
        raise ValueError("Trust not found")

    cursor.execute('SELECT * FROM Beneficiaries WHERE TrustID = ?', (trust_id,))
    beneficiaries = dictfetchall(cursor)

    cursor.execute('SELECT * FROM BeneficiaryTAD WHERE TrustID = ?', (trust_id,))
    tad_records = dictfetchall(cursor)

    header_date = datetime.strptime(hgh['MessageCreateDate'], '%Y-%m-%dT%H:%M:%S')

    lines = []
    gh_line = (f"{hgh['SectionIdentifier']}|{hgh['HeaderType']}|{hgh['MessageCreateDate']}|{hgh['FileLayoutVersion']}|"
               f"{gh_unique_id}|{hgh['SARSRequestReference']}|{hgh['TestDataIndicator']}|{hgh['DataTypeSupplied']}|"
               f"{hgh['ChannelIdentifier']}|{hgh['SourceIdentifier']}|{hgh['SourceSystem']}|{hgh['SourceSystemVersion']}|"
               f"{hgh['ContactPersonName']}|{hgh['ContactPersonSurname']}|{hgh['BusinessTelephoneNumber1']}|"
               f"{hgh['BusinessTelephoneNumber2']}|{hgh['CellPhoneNumber']}|{hgh['ContactEmail']}")
    lines.append(gh_line)

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

    timestamp = header_date.strftime('%Y%m%dT%H%M%S')
    filename = f"I3T_1_1517179642_{gh_unique_id}_{timestamp}_B2BSFG.txt"
    # conn.close()
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
        # conn.close()

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
    conn = get_db_connection()
    tables = ['Beneficiaries', 'BeneficiaryDNT', 'BeneficiaryTAD', 'BeneficiaryTFF', 'HGHHeaders', 'Submissions',
              'Trusts']
    data = {}
    cursor = conn.cursor()
    for table in tables:
        cursor.execute(f'SELECT * FROM {table}')
        data[table] = dictfetchall(cursor)
    # conn.close()

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


@app.route('/import_data', methods=['POST'])
def import_data_post():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)

    file = request.files['file']
    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)

    if file:
        data = json.load(file)
        conn = get_db_connection()
        cursor = conn.cursor()

        tables = ['Beneficiaries', 'BeneficiaryDNT', 'BeneficiaryTAD', 'BeneficiaryTFF', 'FinancialTransactions',
                  'HGHHeaders', 'Submissions', 'Trusts']
        for table in tables:
            cursor.execute(f'DROP TABLE IF EXISTS {table}')

        cursor.execute('''
            CREATE TABLE Trusts (
                TrustID INT IDENTITY(1,1) PRIMARY KEY,
                TrustRegNumber NVARCHAR(50),
                TrustName NVARCHAR(255),
                TaxNumber NVARCHAR(20),
                SubmissionTaxYear NVARCHAR(4),
                PeriodStartDate NVARCHAR(10),
                PeriodEndDate NVARCHAR(10),
                NatureOfPerson NVARCHAR(50),
                TrustType NVARCHAR(50),
                Residency NVARCHAR(10),
                MastersOffice NVARCHAR(50),
                DateRegisteredMastersOffice NVARCHAR(10),
                PhysicalUnitNumber NVARCHAR(50),
                PhysicalComplex NVARCHAR(100),
                PhysicalStreetNumber NVARCHAR(50),
                PhysicalStreet NVARCHAR(100),
                PhysicalSuburb NVARCHAR(100),
                PhysicalCity NVARCHAR(100),
                PhysicalPostalCode NVARCHAR(10),
                PostalSameAsPhysical BIT,
                PostalAddressLine1 NVARCHAR(100),
                PostalAddressLine2 NVARCHAR(100),
                PostalAddressLine3 NVARCHAR(100),
                PostalAddressLine4 NVARCHAR(100),
                PostalCode NVARCHAR(10),
                ContactNumber NVARCHAR(20),
                CellNumber NVARCHAR(20),
                Email NVARCHAR(100),
                RecordStatus NVARCHAR(50)
            )
        ''')

        cursor.execute('''
            CREATE TABLE Beneficiaries (
                BeneficiaryID INT IDENTITY(1,1) PRIMARY KEY,
                TrustID INT,
                TaxReferenceNumber NVARCHAR(20),
                IsNaturalPerson BIT,
                LastName NVARCHAR(100),
                FirstName NVARCHAR(100),
                OtherName NVARCHAR(100),
                Initials NVARCHAR(10),
                DateOfBirth NVARCHAR(10),
                IdentificationType NVARCHAR(50),
                IDNumber NVARCHAR(20),
                PassportNumber NVARCHAR(20),
                PassportCountry NVARCHAR(10),
                PassportIssueDate NVARCHAR(10),
                CompanyIncomeTaxRefNo NVARCHAR(20),
                CompanyRegistrationNumber NVARCHAR(20),
                CompanyRegisteredName NVARCHAR(100),
                NatureOfPerson NVARCHAR(50),
                IsConnectedPerson BIT,
                IsBeneficiary BIT,
                IsFounder BIT,
                IsDonor BIT,
                IsNonResident BIT,
                IsTaxableOnDistributed BIT,
                HasNonTaxableAmounts BIT,
                HasCapitalDistribution BIT,
                MadeDonations BIT,
                MadeContributions BIT,
                ReceivedDonations BIT,
                ReceivedContributions BIT,
                MadeDistributions BIT,
                ReceivedRefunds BIT,
                HasRightOfUse BIT,
                PhysicalUnitNumber NVARCHAR(50),
                PhysicalComplex NVARCHAR(100),
                PhysicalStreetNumber NVARCHAR(50),
                PhysicalStreet NVARCHAR(100),
                PhysicalSuburb NVARCHAR(100),
                PhysicalCity NVARCHAR(100),
                PhysicalPostalCode NVARCHAR(10),
                PostalSameAsPhysical BIT,
                PostalAddressLine1 NVARCHAR(100),
                PostalAddressLine2 NVARCHAR(100),
                PostalAddressLine3 NVARCHAR(100),
                PostalAddressLine4 NVARCHAR(100),
                PostalCode NVARCHAR(10),
                ContactNumber NVARCHAR(20),
                CellNumber NVARCHAR(20),
                Email NVARCHAR(100)
            )
        ''')

        cursor.execute('''
            CREATE TABLE BeneficiaryDNT (
                BeneficiaryID INT PRIMARY KEY,
                LocalDividends FLOAT,
                ExemptForeignDividends FLOAT,
                OtherNonTaxableIncome FLOAT
            )
        ''')

        cursor.execute('''
            CREATE TABLE BeneficiaryTAD (
                TADID INT IDENTITY(1,1) PRIMARY KEY,
                BeneficiaryID INT,
                SourceCode NVARCHAR(50),
                AmountSubjectToTax FLOAT,
                ForeignTaxCredits FLOAT
            )
        ''')

        cursor.execute('''
            CREATE TABLE BeneficiaryTFF (
                BeneficiaryID INT PRIMARY KEY,
                TotalValueOfCapitalDistributed FLOAT,
                TotalExpensesIncurred FLOAT,
                TotalDonationsToTrust FLOAT,
                TotalContributionsToTrust FLOAT,
                TotalDonationsReceivedFromTrust FLOAT,
                TotalContributionsReceivedFromTrust FLOAT,
                TotalDistributionsToTrust FLOAT,
                TotalContributionsRefundedByTrust FLOAT
            )
        ''')

        cursor.execute('''
            CREATE TABLE FinancialTransactions (
                TransactionID INT IDENTITY(1,1) PRIMARY KEY,
                TrustID INT,
                BeneficiaryID INT,
                TransactionType NVARCHAR(50),
                Amount FLOAT,
                Date NVARCHAR(10)
            )
        ''')

        cursor.execute('''
            CREATE TABLE HGHHeaders (
                HGHID INT IDENTITY(1,1) PRIMARY KEY,
                SectionIdentifier NVARCHAR(10),
                HeaderType NVARCHAR(10),
                MessageCreateDate NVARCHAR(20),
                FileLayoutVersion NVARCHAR(10),
                UniqueFileID NVARCHAR(50),
                SARSRequestReference NVARCHAR(50),
                TestDataIndicator NVARCHAR(10),
                DataTypeSupplied NVARCHAR(10),
                ChannelIdentifier NVARCHAR(10),
                SourceIdentifier NVARCHAR(50),
                SourceSystem NVARCHAR(50),
                SourceSystemVersion NVARCHAR(10),
                ContactPersonName NVARCHAR(50),
                ContactPersonSurname NVARCHAR(50),
                BusinessTelephoneNumber1 NVARCHAR(20),
                BusinessTelephoneNumber2 NVARCHAR(20),
                CellPhoneNumber NVARCHAR(20),
                ContactEmail NVARCHAR(100)
            )
        ''')

        cursor.execute('''
            CREATE TABLE Submissions (
                SubmissionID INT IDENTITY(1,1) PRIMARY KEY,
                TrustID INT,
                SubmissionDate NVARCHAR(20),
                Status NVARCHAR(50)
            )
        ''')

        for table, rows in data.items():
            if rows:
                columns = ', '.join(rows[0].keys())
                placeholders = ', '.join(['?' for _ in rows[0]])
                cursor.executemany(f'INSERT INTO {table} ({columns}) VALUES ({placeholders})',
                                   [tuple(row.values()) for row in rows])

        # conn.close()
        flash('Data imported successfully')
        return redirect(url_for('index'))


import subprocess


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
    threading.Timer(1.25, lambda: webbrowser.open('http://127.0.0.1:5000')).start()
    app.run(debug=True)
