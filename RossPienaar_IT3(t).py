import webbrowser
import threading
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pymssql
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
        params = dict(param.split('=') for param in conn_str.split(';') if param)
        g.db = pymssql.connect(
            server=params['server'],
            user=params['user'],
            password=params['password'],
            database=params['database'],
            charset='utf8'
        )
        g.db.autocommit(True)
    return g.db

@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'db'):
        g.db.close()

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
        conn.commit()
        conn.close()
        flash('HGH added!')
        return redirect(url_for('trusts'))
    current_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    unique_file_id = str(uuid.uuid4())
    return render_template('hgh_list.html', current_datetime=current_datetime, unique_file_id=unique_file_id)

@app.route('/hgh_list')
def hgh_list():
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT * FROM HGHHeaders')
    hghs = cursor.fetchall()
    conn.close()
    return render_template('hgh_list.html', hghs=hghs)

@app.route('/edit_hgh/<int:hgh_id>', methods=('GET', 'POST'))
def edit_hgh(hgh_id):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT * FROM HGHHeaders WHERE ID = ?', (hgh_id,))
    hgh = cursor.fetchone()
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
        conn.commit()
        conn.close()
        flash('HGH updated!')
        return redirect(url_for('hgh_list'))
    conn.close()
    current_datetime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return render_template('hgh_form.html', hgh=hgh, current_datetime=current_datetime)

@app.route('/delete_hgh/<int:hgh_id>')
def delete_hgh(hgh_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM HGHHeaders WHERE ID = ?', (hgh_id,))
    conn.commit()
    conn.close()
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
            conn.commit()
        except Exception:
            flash('Trust registration number must be unique.')
            return render_template('add_trust.html')
        finally:
            conn.close()
        return redirect(url_for('trusts'))
    return render_template('add_trust.html')

@app.route('/edit_trust/<int:trust_id>', methods=('GET', 'POST'))
def edit_trust(trust_id):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = cursor.fetchone()
    if trust is None:
        conn.close()
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
        conn.commit()
        conn.close()
        return redirect(url_for('trusts_list'))
    conn.close()
    return render_template('edit_trust.html', trust=trust, mode='capture')

@app.route('/delete_trust/<int:trust_id>')
def delete_trust(trust_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM Trusts WHERE TrustID = ?', (trust_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('trusts'))

@app.route('/add_submission/<int:trust_id>', methods=('GET', 'POST'))
def add_submission(trust_id):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = cursor.fetchone()
    if trust is None:
        conn.close()
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
            conn.commit()
        except Exception as e:
            flash(f'Error adding submission: {str(e)}')
            return render_template('add_submission.html', trust=trust)
        finally:
            conn.close()
        return redirect(url_for('trusts'))
    conn.close()
    return render_template('add_submission.html', trust=trust)

@app.route('/trusts')
def trusts_list():
    mode = request.args.get('mode')
    mode_out = mode
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)

    if mode == 'submissions':
        cursor.execute("SELECT * FROM Trusts WHERE RecordStatus <> '0000 - Imported' ORDER BY RecordStatus DESC")
        mode_out = 'submissions'
    else:
        cursor.execute("SELECT * FROM Trusts WHERE RecordStatus NOT IN ('9010 - SUBMITTED TO SARS') ORDER BY RecordStatus DESC")
        mode_out = 'capture'
    trusts = cursor.fetchall()
    conn.close()

    return render_template('trusts.html', trusts=trusts, mode=mode_out)

@app.route('/edit_submission/<int:submission_id>', methods=('GET', 'POST'))
def edit_submission(submission_id):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT * FROM Submissions WHERE SubmissionID = ?', (submission_id,))
    submission = cursor.fetchone()
    if submission is None:
        conn.close()
        return 'Submission not found', 404
    trust_id = submission['TrustID']
    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = cursor.fetchone()
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
        security_token = request.form.get('SecurityToken', str(uuid.uuid4()))

        if not user_first_name or not user_last_name:
            flash('User First Name and Last Name are required!')
            return render_template('edit_submission.html', submission=submission, trust=trust)

        cursor.execute("""
            UPDATE Submissions SET SubmissionDate = ?, SubmissionType = ?, Status = ?, SoftwareName = ?, SoftwareVersion = ?,
            UserFirstName = ?, UserLastName = ?, UserContactNumber = ?, UserEmail = ?, SecurityToken = ?
            WHERE SubmissionID = ?
        """, (submission_date, submission_type, status, software_name, software_version,
              user_first_name, user_last_name, user_contact_number, user_email, security_token, submission_id))
        conn.commit()
        conn.close()
        return redirect(url_for('trusts', mode='capture'))
    conn.close()
    return render_template('edit_submission.html', submission=submission, trust=trust)

@app.route('/delete_beneficiary/<int:beneficiary_id>')
def delete_beneficiary(beneficiary_id):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT TrustID FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    beneficiary = cursor.fetchone()
    if beneficiary:
        trust_id = beneficiary['TrustID']
        cursor.execute('DELETE FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
        conn.commit()
        conn.close()
        return redirect(url_for('view_beneficiaries', trust_id=trust_id))
    else:
        conn.close()
        return 'Beneficiary not found', 404

@app.route('/beneficiaries/<int:trust_id>')
def view_beneficiaries(trust_id):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = cursor.fetchone()
    cursor.execute('SELECT * FROM Beneficiaries WHERE TrustID = ?', (trust_id,))
    beneficiaries = cursor.fetchall()
    conn.close()
    return render_template('view_beneficiaries.html', trust=trust, beneficiaries=beneficiaries)

@app.route('/add_beneficiary/<int:trust_id>', methods=('GET', 'POST'))
def add_beneficiary(trust_id):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = cursor.fetchone()
    if request.method == 'POST':
        last_name = request.form.get('last_name', '')
        first_name = request.form.get('first_name', '')
        identification_type = request.form.get('identification_type', '')
        nature_of_person = request.form.get('nature_of_person', '')

        if not last_name or not first_name:
            flash('Last Name and First Name are required!')
            return render_template('add_beneficiary.html', trust=trust)

        cursor.execute('''
            INSERT INTO Beneficiaries (TrustID, LastName, FirstName, IdentificationType, NatureOfPerson)
            VALUES (?, ?, ?, ?, ?)
        ''', (trust_id, last_name, first_name, identification_type, nature_of_person))
        conn.commit()
        conn.close()
        return redirect(url_for('view_beneficiaries', trust_id=trust_id))

    return render_template('add_beneficiary.html', trust=trust)

@app.route('/edit_beneficiary/<int:beneficiary_id>', methods=('GET', 'POST'))
def edit_beneficiary(beneficiary_id):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
    beneficiary = cursor.fetchone()
    if beneficiary is None:
        conn.close()
        return 'Beneficiary not found', 404

    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (beneficiary['TrustID'],))
    trust = cursor.fetchone()
    if trust is None:
        conn.close()
        return 'Trust not found', 404

    cursor.execute('SELECT * FROM BeneficiaryTAD WHERE BeneficiaryID = ?', (beneficiary_id,))
    tad_records = cursor.fetchall()

    cursor.execute('SELECT * FROM BeneficiaryDNT WHERE BeneficiaryID = ?', (beneficiary_id,))
    dnt_row = cursor.fetchone()
    dnt_data = dnt_row if dnt_row else {
        'LocalDividends': 0.00,
        'ExemptForeignDividends': 0.00,
        'OtherNonTaxableIncome': 0.00
    }

    cursor.execute('SELECT * FROM BeneficiaryTFF WHERE BeneficiaryID = ?', (beneficiary_id,))
    tff_row = cursor.fetchone()

    if request.method == 'POST':
        source_codes = request.form.getlist('tad_source_code[]')
        amounts = request.form.getlist('tad_amount[]')
        foreign_taxes = request.form.getlist('tad_foreign_tax[]')

        tax_reference_number = request.form['taxReferenceNumber']
        last_name = request.form['lastName']
        first_name = request.form['firstName']
        other_name = request.form['otherName']
        initials = request.form['initials']
        date_of_birth = request.form['dateOfBirth']
        identification_type = request.form['identificationType']
        id_number = request.form['idNumber']
        passport_number = request.form['passportNumber']
        passport_country = request.form['passportCountry']
        passport_issue_date = request.form['passportIssueDate']
        company_income_tax_ref_no = request.form['companyIncomeTaxRefNo']
        company_registration_number = request.form['companyRegistrationNumber']
        company_registered_name = request.form['companyRegisteredName']

        is_connected_person = 'isConnectedPerson' in request.form
        is_beneficiary = 'isBeneficiary' in request.form
        is_founder = 'isFounder' in request.form
        is_donor = 'isDonor' in request.form
        is_non_resident = 'isNonResident' in request.form
        is_taxable_on_distributed = 'isTaxableOnDistributed' in request.form
        has_non_taxable_amounts = 'hasNonTaxableAmounts' in request.form
        has_capital_distribution = 'hasCapitalDistribution' in request.form
        made_donations = 'madeDonations' in request.form
        made_contributions = 'madeContributions' in request.form
        received_donations = 'receivedDonations' in request.form
        received_contributions = 'receivedContributions' in request.form
        made_distributions = 'madeDistributions' in request.form
        received_refunds = 'receivedRefunds' in request.form
        has_right_of_use = 'hasRightOfUse' in request.form

        physical_unit_number = request.form['physicalUnitNumber']
        physical_complex = request.form['physicalComplex']
        physical_street_number = request.form['physicalStreetNumber']
        physical_street = request.form['physicalStreet']
        physical_suburb = request.form['physicalSuburb']
        physical_city = request.form['physicalCity']
        physical_postal_code = request.form['physicalPostalCode']
        postal_same_as_physical = 'postalSameAsPhysical' in request.form
        postal_address_line1 = request.form['postalAddressLine1']
        postal_address_line2 = request.form['postalAddressLine2']
        postal_address_line3 = request.form['postalAddressLine3']
        postal_address_line4 = request.form['postalAddressLine4']
        postal_code = request.form['postalCode']
        telephone_number = request.form['contactNumber']
        cell_phone_number = request.form['cellNumber']
        email = request.form['email']

        if identification_type == 'South African ID' and id_number and not sa_id_check(id_number, date_of_birth):
            flash('Invalid South African ID Number or Date of Birth mismatch!')
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_row)

        if tax_reference_number and not modulus_10_check(tax_reference_number):
            flash('Invalid Tax Reference Number!')
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_row)

        try:
            conn.execute('BEGIN TRANSACTION')

            cursor.execute(
                'UPDATE Beneficiaries SET '
                'TaxReferenceNumber = ?, LastName = ?, FirstName = ?, OtherName = ?, Initials = ?, DateOfBirth = ?, '
                'IdentificationType = ?, IDNumber = ?, PassportNumber = ?, PassportCountry = ?, PassportIssueDate = ?, '
                'CompanyIncomeTaxRefNo = ?, CompanyRegistrationNumber = ?, CompanyRegisteredName = ?, NatureOfPerson = ?, '
                'IsConnectedPerson = ?, IsBeneficiary = ?, IsFounder = ?, IsDonor = ?, IsNonResident = ?, '
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
                    request.form.get('identificationType'),
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

            cursor.execute('DELETE FROM BeneficiaryTAD WHERE BeneficiaryID = ?', (beneficiary_id,))

            if len(source_codes) == len(amounts) == len(foreign_taxes):
                for sc, amt, ft in zip(source_codes, amounts, foreign_taxes):
                    try:
                        amt_val = float(amt) if amt.strip() else 0.0
                        ft_val = float(ft) if ft.strip() else 0.0
                    except ValueError:
                        conn.rollback()
                        flash('Invalid amount or foreign tax value! Please enter valid numeric values.')
                        return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                               tad_records=tad_records, tff_data=tff_row)

                    if amt_val >= 0 and ft_val >= 0:
                        cursor.execute(
                            'INSERT INTO BeneficiaryTAD (SectionIdentifier, RecordType, RecordStatus, UniqueNumber, RowNumber, BeneficiaryID, AmountSubjectToTax, SourceCode, ForeignTaxCredits, TrustID) '
                            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                            ('B', 'TAD', 'N', str(uuid.uuid4()), '', beneficiary_id, amt_val, sc, ft_val,
                             beneficiary['TrustID'])
                        )

            cursor.execute('DELETE FROM BeneficiaryDNT WHERE BeneficiaryID = ?', (beneficiary_id,))

            if request.form.get('hasNonTaxableAmounts'):
                try:
                    local_dividends = float(request.form.get('local_dividends', 0.00)) if request.form.get('local_dividends', '').strip() else 0.0
                    exempt_foreign_dividends = float(request.form.get('exempt_foreign_dividends', 0.00)) if request.form.get('exempt_foreign_dividends', '').strip() else 0.0
                    other_non_taxable = float(request.form.get('other_non_taxable', 0.00)) if request.form.get('other_non_taxable', '').strip() else 0.0
                except ValueError:
                    conn.rollback()
                    flash('Invalid non-taxable amount values! Please enter valid numeric values.')
                    return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                           tad_records=tad_records, tff_data=tff_row)

                cursor.execute(
                    'INSERT INTO BeneficiaryDNT (SectionIdentifier, RecordType, RecordStatus, BeneficiaryID, LocalDividends, ExemptForeignDividends, OtherNonTaxableIncome, TrustID) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    ('B', 'DNT', 'N', beneficiary_id, local_dividends, exempt_foreign_dividends, other_non_taxable,
                     beneficiary['TrustID'])
                )

            cursor.execute('DELETE FROM BeneficiaryTFF WHERE BeneficiaryID = ?', (beneficiary_id,))

            try:
                total_value_of_capital_distributed = float(request.form.get('total_value_of_capital_distributed', 0.00)) if request.form.get('total_value_of_capital_distributed', '').strip() else 0.0
                total_expenses_incurred = float(request.form.get('total_expenses_incurred', 0.00)) if request.form.get('total_expenses_incurred', '').strip() else 0.0
                total_donations_to_trust = float(request.form.get('donations_made', 0.00)) if request.form.get('donations_made', '').strip() else 0.0
                total_contributions_to_trust = float(request.form.get('total_value_of_contributions_made', 0.00)) if request.form.get('total_value_of_contributions_made', '').strip() else 0.0
                total_donations_received = float(request.form.get('donations_received', 0.00)) if request.form.get('donations_received', '').strip() else 0.0
                total_contributions_received = float(request.form.get('contributions_received', 0.00)) if request.form.get('contributions_received', '').strip() else 0.0
                total_distributions_to_trust = float(request.form.get('distributions_made', 0.00)) if request.form.get('distributions_made', '').strip() else 0.0
                total_contributions_refunded = float(request.form.get('refunds_received', 0.00)) if request.form.get('refunds_received', '').strip() else 0.0
            except ValueError:
                conn.rollback()
                flash('Invalid TFF values! Please enter valid numeric values.')
                return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                       tad_records=tad_records, tff_data=tff_row)

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

            conn.commit()
            flash('Beneficiary updated successfully!', 'success')
            return redirect(url_for('view_beneficiaries', trust_id=beneficiary['TrustID']))

        except Exception as e:
            conn.rollback()
            flash(f'Unexpected error: {str(e)}')
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_row)

    return render_template('edit_beneficiary.html', beneficiary=beneficiary, tad_records=tad_records, tff_data=tff_row, trust=trust)

@app.route('/mark_ready_for_submission/<int:trust_id>', methods=['POST'])
def mark_ready_for_submission(trust_id):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE Trusts SET RecordStatus = '9001 - Ready for Submission' WHERE TrustID = ?", (trust_id,))
        cursor.execute("UPDATE Beneficiaries SET RecordStatus = '9001 - Ready for Submission' WHERE TrustID = ?", (trust_id,))
        conn.commit()
        flash('Trust and Beneficiaries marked as Ready for Submission.', 'success')
    except Exception as e:
        flash(f'Error updating records: {str(e)}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('view_beneficiaries', trust_id=trust_id))

@app.route('/generate_i3t/<int:trust_id>')
def generate_i3t(trust_id):
    gh_unique_id = request.args.get('gh_unique_id', str(uuid.uuid4()))
    filename, file_content, trust, dpb_count, tad_count, tff_count, total_amount = generate_file_content(trust_id, gh_unique_id)
    return render_template('generate_i3t.html', trust_id=trust_id, trust=trust, filename=filename, dpb_count=dpb_count,
                           tad_count=tad_count, tff_count=tff_count, total_amount=total_amount,
                           file_content=file_content, gh_unique_id=gh_unique_id)

def generate_file_content(trust_id, gh_unique_id):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,))
    trust = cursor.fetchone()
    if not trust:
        raise ValueError('Trust not found')

    cursor.execute('SELECT * FROM Submissions WHERE TrustID = ? ORDER BY SubmissionDate DESC', (trust_id,))
    submission = cursor.fetchone()
    if not submission:
        submission = {
            'SoftwareName': 'GreatSoft',
            'SoftwareVersion': '2024.3.1',
            'UserFirstName': 'Karin',
            'UserLastName': 'Roux',
            'BusinessTelephoneNumber1': '0123428393',
            'BusinessTelephoneNumber2': '0606868076',
            'CellPhoneNumber': '',
            'UserEmail': 'karin@rosspienaar.co.za',
            'SecurityToken': '9CD036C9-210F-40C5-91F7-82959AB269C02228AE14-53A7-4AC2-A539-A20D1D5654E6F4A52305-B91E-4435-B25E-46E38A528398D60DAB72-1C44-4490-99AF-FA572D3AFC69',
            'SubmissionDate': datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        }

    cursor.execute('SELECT * FROM Beneficiaries WHERE TrustID = ?', (trust_id,))
    beneficiaries = cursor.fetchall()
    dpb_count = len(beneficiaries)

    cursor.execute('SELECT * FROM BeneficiaryTAD WHERE TrustID = ?', (trust_id,))
    tad_records = cursor.fetchall()
    tad_count = len(tad_records)

    cursor.execute('SELECT * FROM BeneficiaryTFF WHERE TrustID = ?', (trust_id,))
    tff_records = cursor.fetchall()
    tff_count = len(tff_records)

    lines = []
    sequence_number = 1
    ri_unique_id = str(uuid.uuid4())
    header_date = datetime.now()

    gh_line = f"H|GH|{header_date.strftime('%Y-%m-%dT%H:%M:%S')}|1|{gh_unique_id}||T|I3T|HTTPS|9CD036C9-210F-40C5-91F7-82959AB269C02228AE14-53A7-4AC2-A539-A20D1D5654E6F4A52305-B91E-4435-B25E-46E38A528398D60DAB72-1C44-4490-99AF-FA572D3AFC69|GreatSoft|2024.3.1|Karin|Roux|0123428393|0606868076|0823714777|karin@rosspienaar.co.za"
    lines.append(gh_line)

    se_line = f"H|SE|{trust['SubmissionTaxYear']}|{trust['PeriodStartDate'] or '2024-03-01'}|{trust['PeriodEndDate'] or '2025-02-28'}|INDIVIDUAL|Pienaar|DJ|Daniel Jacobus||001|6307205099081|ZA|00212071|SAICA|1517179642||PO BOX 35336|MENLOPARK|||0102|0123428393|0606868076|tax@rosspienaar.co.za"
    lines.append(se_line)

    ri_line = f"B|RI|N|{ri_unique_id}|{sequence_number}|{trust['NatureOfPerson']}|{trust['TrustName']}|{trust['TrustRegNumber']}|{trust['DateRegisteredMastersOffice'].replace('-', '')}|{trust['TaxNumber'] or ''}|{''}|{trust['Residency'] or 'ZA'}|{trust['MastersOffice'] or ''}|{trust['TrustType'] or ''}|{trust['PhysicalUnitNumber'] or ''}|{trust['PhysicalComplex'] or ''}|{trust['PhysicalStreetNumber'] or ''}|{trust['PhysicalStreet']}|{trust['PhysicalSuburb'] or ''}|{trust['PhysicalCity']}|{trust['PhysicalPostalCode']}|{trust['PostalSameAsPhysical'] and 'Y' or 'N'}|{trust['PostalAddressLine1'] or ''}|{trust['PostalAddressLine2'] or ''}|{trust['PostalAddressLine3'] or ''}|{trust['PostalAddressLine4'] or ''}|{trust['PostalCode'] or ''}|{trust['ContactNumber'] or '999999999999999'}|{trust['CellNumber'] or '999999999999999'}|{trust['Email'] or ''}"
    lines.append(ri_line)
    sequence_number += 1

    for beneficiary in beneficiaries:
        dpb_unique_id = beneficiary['UniqueRecordID'] or str(uuid.uuid4())
        dpb_line = (f"B|DPB|N|{dpb_unique_id}|{sequence_number}|{ri_unique_id}"
                    f"|{beneficiary['IsConnectedPerson'] and 'Y' or 'N'}"
                    f"|{beneficiary['IsBeneficiary'] and 'Y' or 'N'}"
                    f"|{beneficiary['IsFounder'] and 'Y' or 'N'}"
                    f"|{beneficiary['NatureOfPerson'] == '1' and 'Y' or 'N'}"
                    f"|{beneficiary['IsDonor'] and 'Y' or 'N'}"
                    f"|{beneficiary['IsNonResident'] and 'Y' or 'N'}"
                    f"|{beneficiary['TaxReferenceNumber'] or ''}"
                    f"|{beneficiary['LastName']}"
                    f"|{beneficiary['FirstName']}"
                    f"|{beneficiary['OtherName'] or ''}"
                    f"|{beneficiary['Initials'] or ''}"
                    f"|{(beneficiary['DateOfBirth'] or '').replace('-', '')}"
                    f"|{beneficiary['IDNumber'] or ''}"
                    f"|{beneficiary['PassportNumber'] or ''}"
                    f"|{beneficiary['PassportCountry'] or ''}"
                    f"|{beneficiary['PassportIssueDate'] or ''}"
                    f"|{sanitize(beneficiary['CompanyIncomeTaxRefNo'])}"
                    f"|{sanitize(beneficiary['CompanyRegistrationNumber'])}"
                    f"|{sanitize(beneficiary['CompanyRegisteredName'])}"
                    f"|{beneficiary['PhysicalUnitNumber'] or ''}"
                    f"|{beneficiary['PhysicalComplex'] or ''}"
                    f"|{beneficiary['PhysicalStreetNumber'] or ''}"
                    f"|{beneficiary['PhysicalStreet'] or ''}"
                    f"|{beneficiary['PhysicalSuburb'] or ''}"
                    f"|{beneficiary['PhysicalCity'] or ''}"
                    f"|{beneficiary['PhysicalPostalCode'] or ''}"
                    f"|{beneficiary['PostalSameAsPhysical'] and 'Y' or 'N'}"
                    f"|{beneficiary['PostalAddressLine1'] or ''}"
                    f"|{beneficiary['PostalAddressLine2'] or ''}"
                    f"|{beneficiary['PostalAddressLine3'] or ''}"
                    f"|{beneficiary['PostalAddressLine4'] or ''}"
                    f"|{beneficiary['PostalCode'] or ''}"
                    f"|{beneficiary['ContactNumber'] or '999999999999999'}"
                    f"|{beneficiary['CellNumber'] or '999999999999999'}"
                    f"|{beneficiary['Email'] or ''}"
                    f"|{beneficiary['IsTaxableOnDistributed'] and 'Y' or 'N'}"
                    f"|{beneficiary['HasNonTaxableAmounts'] and 'Y' or 'N'}"
                    f"|{beneficiary['HasCapitalDistribution'] and 'Y' or 'N'}"
                    f"|{beneficiary['MadeDonations'] and 'Y' or 'N'}"
                    f"|{beneficiary['MadeContributions'] and 'Y' or 'N'}"
                    f"|{beneficiary['ReceivedDonations'] and 'Y' or 'N'}"
                    f"|{beneficiary['ReceivedContributions'] and 'Y' or 'N'}"
                    f"|{beneficiary['MadeDistributions'] and 'Y' or 'N'}"
                    f"|{beneficiary['ReceivedRefunds'] and 'Y' or 'N'}"
                    f"|{beneficiary['HasRightOfUse'] and 'Y' or 'N'}")
        lines.append(dpb_line)
        sequence_number += 1

        cursor.execute('SELECT * FROM BeneficiaryTAD WHERE BeneficiaryID = ?', (beneficiary['BeneficiaryID'],))
        tad_records1 = cursor.fetchall()

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
        dnt_row = cursor.fetchone()

        dnt_data = dict(dnt_row) if dnt_row else {
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
        tff_row = cursor.fetchall()

        totalValueOfCapitalDistributed = int(sum(float(row['TotalValueOfCapitalDistributed']) for row in tff_row))
        totalExpensesIncurred = int(sum(float(row['TotalExpensesIncurred']) for row in tff_row))
        totalDonationsToTrust = int(sum(float(row['TotalDonationsToTrust']) for row in tff_row))
        totalContributionsToTrust = int(sum(float(row['TotalContributionsToTrust']) for row in tff_row))
        totalDonationsReceivedFromTrust = int(sum(float(row['TotalDonationsReceivedFromTrust']) for row in tff_row))
        totalContributionsReceivedFromTrust = int(sum(float(row['TotalContributionsReceivedFromTrust']) for row in tff_row))
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
    dnt_row = cursor.fetchall()

    cursor.execute('SELECT * FROM BeneficiaryTFF WHERE TrustID = ?', (trust_id,))
    tff_row = cursor.fetchall()

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
    return filename, '\n'.join(lines), trust, dpb_count, tad_count, tff_count, total_amount

@app.route('/generate_i3t_direct/<int:trust_id>')
def generate_i3t_direct(trust_id):
    try:
        gh_unique_id = str(uuid.uuid4())
        filename, file_content, trust, dpb_count, tad_count, tff_count, total_amount = generate_file_content(trust_id, gh_unique_id)
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE Trusts SET RecordStatus = '9001 - Submitted to SARS' WHERE TrustID = ?", (trust_id,))
        conn.commit()
        conn.close()

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
    tables = ['Beneficiaries', 'BeneficiaryDNT', 'BeneficiaryTAD', 'BeneficiaryTFF', 'HGHHeaders', 'Submissions', 'Trusts']
    data = {}
    cursor = conn.cursor(as_dict=True)
    for table in tables:
        cursor.execute(f'SELECT * FROM {table}')
        data[table] = [dict(row) for row in cursor.fetchall()]
    conn.close()

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

        conn.commit()
        conn.close()
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