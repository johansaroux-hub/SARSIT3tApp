import webbrowser
import threading
from flask import Flask, render_template, request, redirect, url_for, flash
import pymssql
import os
import io
from datetime import datetime
import logging
import uuid
import json
import hashlib
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'fallback-secret-key')

logging.basicConfig(filename=os.path.join(os.getcwd(), os.path.dirname(__file__), 'app_errors.log'), level=logging.DEBUG)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'beneficial_ownership.db')


def sanitize(value):
    return '' if value in (None, 'None', 'none') else str(value)


def generate_it3t_pdf_from_flatfile(dpb_data, trust_data, transaction_data, output_buffer):
    doc = SimpleDocTemplate(
        output_buffer,
        pagesize=letter,
        topMargin=90,
        bottomMargin=90,
        leftMargin=96,
        rightMargin=96
    )
    styles = getSampleStyleSheet()
    story = []

    # Page 1: Title
    story.append(Spacer(1, 100))
    story.append(PageBreak())

    # Page 2: Placeholder
    story.append(PageBreak())

    # Page 3: Certificate
    story.append(Paragraph("Tax Certificate Information", styles['Heading1']))
    story.append(Spacer(1, 12))
    story.append(Paragraph(f"Issued by: {trust_data['TrustName']}", styles['Normal']))
    story.append(Paragraph(f"Trust Registration Number: {trust_data['TrustRegNumber']}", styles['Normal']))
    story.append(Paragraph(f"Tax Number: {trust_data.get('TaxNumber', 'N/A')}", styles['Normal']))
    story.append(Paragraph("Tax Year: 2025 (2024-03-01 to 2025-02-28)", styles['Normal']))
    story.append(Paragraph(f"Submission Date: {datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}", styles['Normal']))
    story.append(Spacer(1, 12))

    story.append(Paragraph("Taxpayer Details:", styles['Heading2']))
    story.append(Paragraph(f"- Name: {dpb_data['LastName']}, {dpb_data['FirstName']}", styles['Normal']))
    story.append(Paragraph(f"- Tax Reference Number: {dpb_data.get('TaxReferenceNumber', 'N/A')}", styles['Normal']))
    story.append(Paragraph(f"- ID Number/Date of Birth: {dpb_data.get('IDNumber', dpb_data.get('DateOfBirth', 'N/A'))}",
                           styles['Normal']))
    story.append(Spacer(1, 12))

    story.append(Paragraph("Transaction Details:", styles['Heading2']))
    for tx in transaction_data.get('TAD', []):
        story.append(Paragraph(
            f"- TAD Code: {tx['Code']}, Amount: {tx['Amount']:.2f}, Foreign Tax Credits: {tx['ForeignTaxCredits']:.2f}",
            styles['Normal']))
    for tx in transaction_data.get('TFF', []):
        story.append(Paragraph(f"- TFF Values: {', '.join(tx['Values'])}", styles['Normal']))

    total_amount = sum(tx['Amount'] for tx in transaction_data.get('TAD', []))

    story.append(Paragraph(f"- Total Amount: {total_amount:.2f}", styles['Normal']))
    story.append(Spacer(1, 12))

    story.append(Paragraph("Declaration:", styles['Heading2']))
    story.append(Paragraph(
        "This is to certify that the above details are true and correct as per the IT3(t) submission for the 2025 tax year.",
        styles['Normal']))
    story.append(Paragraph(f"Unique Record ID: {dpb_data.get('UniqueRecordID', 'N/A')}", styles['Normal']))

    doc.build(story)


from flask import g


def get_db_connection():
    if 'db' not in g:
        conn_str = os.getenv('AZURE_SQL_CONNECTION_STRING')
        if conn_str:
            params = dict(param.split('=') for param in conn_str.split(';') if param)
            g.db = pymssql.connect(
                server=params['server'],
                user=params['user'],
                password=params['password'],
                database=params['database'],
                charset='utf8'
            )
            g.db.autocommit(True)
        else:
            # Fallback to SQLite for local testing
            g.db = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'beneficial_ownership.db'), timeout=10)
            g.db.row_factory = sqlite3.Row
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
    # conn = get_db_connection()
    # trusts = conn.execute('SELECT * FROM Trusts').fetchall()
    # conn.close()
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
    hghs = conn.execute('SELECT * FROM HGHHeaders').fetchall()

    return render_template('hgh_list.html', hghs=hghs)


@app.route('/edit_hgh/<int:hgh_id>', methods=('GET', 'POST'))
def edit_hgh(hgh_id):
    conn = get_db_connection()
    hgh = conn.execute('SELECT * FROM HGHHeaders WHERE ID = ?', (hgh_id,)).fetchone()
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

        conn.execute("""
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
    conn.execute('DELETE FROM HGHHeaders WHERE ID = ?', (hgh_id,))
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
        except sqlite3.IntegrityError:
            flash('Trust registration number must be unique.')
            return render_template('add_trust.html')
        finally:
            conn.close()
        return redirect(url_for('trusts'))
    return render_template('add_trust.html')


@app.route('/edit_trust/<int:trust_id>', methods=('GET', 'POST'))
def edit_trust(trust_id):
    conn = get_db_connection()
    trust = conn.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,)).fetchone()
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
        conn.close()
        return redirect(url_for('trusts_list'))
    conn.close()
    return render_template('edit_trust.html', trust=trust, mode='capture')


@app.route('/delete_trust/<int:trust_id>')
def delete_trust(trust_id):
    conn = get_db_connection()
    conn.execute('DELETE FROM Trusts WHERE TrustID = ?', (trust_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('trusts'))


@app.route('/add_submission/<int:trust_id>', methods=('GET', 'POST'))
def add_submission(trust_id):
    conn = get_db_connection()
    trust = conn.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,)).fetchone()
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

        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO Submissions (TrustID, SubmissionDate, SubmissionType, Status, SoftwareName, SoftwareVersion,
                UserFirstName, UserLastName, UserContactNumber, UserEmail, SecurityToken)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (trust_id, submission_date, submission_type, status, software_name, software_version,
                  user_first_name, user_last_name, user_contact_number, user_email, security_token))
            conn.commit()
        except sqlite3.Error as e:
            flash(f'Error adding submission: {str(e)}')
            return render_template('add_submission.html', trust=trust)
        finally:
            conn.close()
        return redirect(url_for('trusts'))
    conn.close()
    return render_template('add_submission.html', trust=trust)


@app.route('/trusts')
def trusts_list():
    mode = request.args.get('mode')  # Retrieve the 'mode' query parameter (e.g., 'capture' or 'submissions')
    mode_out = mode
    conn = get_db_connection()

    if mode == 'submissions':
        trusts = conn.execute(
            "SELECT * FROM Trusts where RecordStatus <> '0000 - Imported' order by RecordStatus DESC").fetchall()
        mode_out = 'submissions'
    else:
        # Default or 'capture' mode: Fetch all trusts
        trusts = conn.execute(
            "SELECT * FROM Trusts where RecordStatus not in ('9010 - SUBMITTED TO SARS') order by RecordStatus DESC").fetchall()
        mode_out = 'capture'
    conn.close()

    print("mode_out =", mode_out)

    return render_template('trusts.html', trusts=trusts, mode=mode_out)


@app.route('/edit_submission/<int:submission_id>', methods=('GET', 'POST'))
def edit_submission(submission_id):
    conn = get_db_connection()
    submission = conn.execute('SELECT * FROM Submissions WHERE SubmissionID = ?', (submission_id,)).fetchone()
    if submission is None:
        conn.close()
        return 'Submission not found', 404
    trust_id = submission['TrustID']
    trust = conn.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,)).fetchone()
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

        conn.execute("""
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
    beneficiary = conn.execute('SELECT TrustID FROM Beneficiaries WHERE BeneficiaryID = ?',
                               (beneficiary_id,)).fetchone()
    if beneficiary:
        trust_id = beneficiary['TrustID']
        conn.execute('DELETE FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,))
        conn.commit()
        conn.close()
        return redirect(url_for('view_beneficiaries', trust_id=trust_id))
    else:
        conn.close()
        return 'Beneficiary not found', 404


@app.route('/beneficiaries/<int:trust_id>')
def view_beneficiaries(trust_id):
    conn = get_db_connection()
    trust = conn.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,)).fetchone()
    beneficiaries = conn.execute('SELECT * FROM Beneficiaries WHERE TrustID = ?', (trust_id,)).fetchall()
    conn.close()
    return render_template('view_beneficiaries.html', trust=trust, beneficiaries=beneficiaries)


@app.route('/add_beneficiary/<int:trust_id>', methods=('GET', 'POST'))
def add_beneficiary(trust_id):
    conn = get_db_connection()
    trust = conn.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,)).fetchone()
    if request.method == 'POST':
        last_name = request.form.get('last_name', '')
        first_name = request.form.get('first_name', '')
        identification_type = request.form.get('identification_type', '')
        nature_of_person = request.form.get('nature_of_person', '')

        if not last_name or not first_name:
            flash('Last Name and First Name are required!')
            return render_template('add_beneficiary.html', trust=trust)

        conn.execute('''
            INSERT INTO Beneficiaries (TrustID, LastName, FirstName, IdentificationType, NatureOfPerson)
            VALUES (?, ?, ?, ?, ?)
        ''', (trust_id, last_name, first_name, identification_type, nature_of_person))
        conn.commit()
        conn.close()
        # return redirect(url_for('view_beneficiaries', trust_id=trust_id))

    return render_template('add_beneficiary.html', trust=trust)


@app.route('/edit_beneficiary/<int:beneficiary_id>', methods=('GET', 'POST'))
def edit_beneficiary(beneficiary_id):
    conn = get_db_connection()
    beneficiary = conn.execute('SELECT * FROM Beneficiaries WHERE BeneficiaryID = ?', (beneficiary_id,)).fetchone()
    if beneficiary is None:
        conn.close()
        return 'Beneficiary not found', 404

    # Fetch trust details
    trust = conn.execute('SELECT * FROM Trusts WHERE TrustID = ?', (beneficiary['TrustID'],)).fetchone()
    if trust is None:
        conn.close()
        return 'Trust not found', 404

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
        source_codes = request.form.getlist('tad_source_code[]')
        amounts = request.form.getlist('tad_amount[]')
        foreign_taxes = request.form.getlist('tad_foreign_tax[]')

        # Log form data for debugging
        logging.debug(f"Source Codes: {source_codes}")
        logging.debug(f"Amounts: {amounts}")
        logging.debug(f"Foreign Taxes: {foreign_taxes}")

        # Update beneficiary details
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

        # Validation
        logging.debug(
            f"ID Number: {id_number}, Date of Birth: {date_of_birth}, Identification Type: {identification_type}")
        logging.debug(f"SA ID Check: {sa_id_check(id_number, date_of_birth)}")

        if identification_type == 'South African ID' and id_number and not sa_id_check(id_number, date_of_birth):
            flash('Invalid South African ID Number or Date of Birth mismatch!')
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_row)

        if tax_reference_number and not modulus_10_check(tax_reference_number):
            flash('Invalid Tax Reference Number!')
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_row)

        try:
            # Begin transaction
            conn.execute('BEGIN TRANSACTION')

            # Update beneficiary
            conn.execute(
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

            # Process TAD records (upsert strategy)
            conn.execute('DELETE FROM BeneficiaryTAD WHERE BeneficiaryID = ?', (beneficiary_id,))

            source_codes = request.form.getlist('tad_source_code[]')
            amounts = request.form.getlist('tad_amount[]')
            foreign_taxes = request.form.getlist('tad_foreign_tax[]')

            if len(source_codes) == len(amounts) == len(foreign_taxes):
                for sc, amt, ft in zip(source_codes, amounts, foreign_taxes):
                    # Handle empty or invalid inputs
                    try:
                        amt_val = float(amt) if amt.strip() else 0.0
                        ft_val = float(ft) if ft.strip() else 0.0
                        logging.debug(f"Inserting TAD: SourceCode={sc}, Amount={amt_val}, ForeignTax={ft_val}")
                    except ValueError:
                        logging.error(f"Invalid amount or foreign tax value: Amount={amt}, ForeignTax={ft}")
                        conn.rollback()
                        flash('Invalid amount or foreign tax value! Please enter valid numeric values.')
                        return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                               tad_records=tad_records, tff_data=tff_row)

                    if amt_val >= 0 and ft_val >= 0:
                        conn.execute(
                            'INSERT INTO BeneficiaryTAD (SectionIdentifier, RecordType, RecordStatus, UniqueNumber, RowNumber, BeneficiaryID, AmountSubjectToTax, SourceCode, ForeignTaxCredits, TrustID) '
                            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                            ('B', 'TAD', 'N', str(uuid.uuid4()), '', beneficiary_id, amt_val, sc, ft_val,
                             beneficiary['TrustID'])
                        )

            # Process DNT records (conditional on HasNonTaxableAmounts)
            conn.execute('DELETE FROM BeneficiaryDNT WHERE BeneficiaryID = ?', (beneficiary_id,))

            if request.form.get('hasNonTaxableAmounts'):
                try:
                    local_dividends = float(request.form.get('local_dividends', 0.00)) if request.form.get(
                        'local_dividends', '').strip() else 0.0
                    exempt_foreign_dividends = float(
                        request.form.get('exempt_foreign_dividends', 0.00)) if request.form.get(
                        'exempt_foreign_dividends', '').strip() else 0.0
                    other_non_taxable = float(request.form.get('other_non_taxable', 0.00)) if request.form.get(
                        'other_non_taxable', '').strip() else 0.0
                    logging.debug(
                        f"Inserting DNT: LocalDividends={local_dividends}, ExemptForeignDividends={exempt_foreign_dividends}, OtherNonTaxable={other_non_taxable}")
                except ValueError:
                    logging.error(
                        f"Invalid DNT values: LocalDividends={request.form.get('local_dividends')}, ExemptForeignDividends={request.form.get('exempt_foreign_dividends')}, OtherNonTaxable={request.form.get('other_non_taxable')}")
                    conn.rollback()
                    flash('Invalid non-taxable amount values! Please enter valid numeric values.')
                    return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                           tad_records=tad_records, tff_data=tff_row)

                conn.execute(
                    'INSERT INTO BeneficiaryDNT (SectionIdentifier, RecordType, RecordStatus, BeneficiaryID, LocalDividends, ExemptForeignDividends, OtherNonTaxableIncome, TrustID) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    ('B', 'DNT', 'N', beneficiary_id, local_dividends, exempt_foreign_dividends, other_non_taxable,
                     beneficiary['TrustID'])
                )

            # Process TFF records
            conn.execute('DELETE FROM BeneficiaryTFF WHERE BeneficiaryID = ?', (beneficiary_id,))

            try:
                total_value_of_capital_distributed = float(
                    request.form.get('total_value_of_capital_distributed', 0.00)) if request.form.get(
                    'total_value_of_capital_distributed', '').strip() else 0.0
                total_expenses_incurred = float(request.form.get('total_expenses_incurred', 0.00)) if request.form.get(
                    'total_expenses_incurred', '').strip() else 0.0
                total_donations_to_trust = float(request.form.get('donations_made', 0.00)) if request.form.get(
                    'donations_made', '').strip() else 0.0
                total_contributions_to_trust = float(
                    request.form.get('total_value_of_contributions_made', 0.00)) if request.form.get(
                    'total_value_of_contributions_made', '').strip() else 0.0
                total_donations_received = float(request.form.get('donations_received', 0.00)) if request.form.get(
                    'donations_received', '').strip() else 0.0
                total_contributions_received = float(
                    request.form.get('contributions_received', 0.00)) if request.form.get('contributions_received',
                                                                                          '').strip() else 0.0
                total_distributions_to_trust = float(request.form.get('distributions_made', 0.00)) if request.form.get(
                    'distributions_made', '').strip() else 0.0
                total_contributions_refunded = float(request.form.get('refunds_received', 0.00)) if request.form.get(
                    'refunds_received', '').strip() else 0.0
                logging.debug(
                    f"Inserting TFF: TotalValueOfCapitalDistributed={total_value_of_capital_distributed}, TotalExpensesIncurred={total_expenses_incurred}")
            except ValueError:
                logging.error(
                    f"Invalid TFF values: TotalValueOfCapitalDistributed={request.form.get('total_value_of_capital_distributed')}, TotalExpensesIncurred={request.form.get('total_expenses_incurred')}")
                conn.rollback()
                flash('Invalid TFF values! Please enter valid numeric values.')
                return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                       tad_records=tad_records, tff_data=tff_row)

            conn.execute(
                'INSERT INTO BeneficiaryTFF (SectionIdentifier, RecordType, RecordStatus, BeneficiaryID, '
                'TotalValueOfCapitalDistributed, TotalExpensesIncurred, TotalDonationsToTrust, TotalContributionsToTrust, '
                'TotalDonationsReceivedFromTrust, TotalContributionsReceivedFromTrust, TotalDistributionsToTrust, TotalContributionsRefundedByTrust, TrustID) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                ('B', 'TFF', 'N', beneficiary_id, total_value_of_capital_distributed, total_expenses_incurred,
                 total_donations_to_trust, total_contributions_to_trust, total_donations_received,
                 total_contributions_received, total_distributions_to_trust, total_contributions_refunded,
                 beneficiary['TrustID'])
            )

            # Commit transaction
            conn.commit()
            logging.info(f"Beneficiary {beneficiary_id} updated successfully")
            flash('Beneficiary updated successfully!', 'success')
            return redirect(url_for('view_beneficiaries', trust_id=beneficiary['TrustID']))

        except sqlite3.OperationalError as e:
            conn.rollback()
            logging.error(f"Database error: {str(e)}")
            flash(f'Database error: {str(e)}')
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_row)
        except Exception as e:
            conn.rollback()
            logging.error(f"Unexpected error: {str(e)}")
            flash(f'Unexpected error: {str(e)}')
            return render_template('edit_beneficiary.html', beneficiary=beneficiary, trust=trust,
                                   tad_records=tad_records, tff_data=tff_row)

    return render_template('edit_beneficiary.html', beneficiary=beneficiary, tad_records=tad_records, dnt_data=dnt_data,
                           tff_data=tff_row, trust=trust)


@app.route('/mark_ready_for_submission/<int:trust_id>', methods=['POST'])
def mark_ready_for_submission(trust_id):
    conn = get_db_connection()
    try:
        # Update Trust record
        conn.execute("UPDATE Trusts SET RecordStatus = '9001 - Ready for Submission' WHERE TrustID = ?", (trust_id,))

        # Update Beneficiary records
        conn.execute("UPDATE Beneficiaries SET RecordStatus = '9001 - Ready for Submission' WHERE TrustID = ?",
                     (trust_id,))

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
    filename, file_content, trust, beneficiaries, dpb_count, tad_count, tff_count, total_amount = generate_file_content(
        trust_id, gh_unique_id)
    return render_template('generate_i3t.html', trust_id=trust_id, trust=trust, filename=filename, dpb_count=dpb_count,
                           tad_count=tad_count, tff_count=tff_count, total_amount=total_amount,
                           file_content=file_content, gh_unique_id=gh_unique_id)


def generate_it3t_pdf(beneficiary_id, trust, beneficiaries, transactions, output_buffer):
    doc = SimpleDocTemplate(
        output_buffer,
        pagesize=letter,
        topMargin=90,
        bottomMargin=90,
        leftMargin=96,
        rightMargin=96
    )
    styles = getSampleStyleSheet()

    # Define page templates
    def first_page(canvas, doc):
        logging.debug(f"Rendering first page, frame height: {doc.height}, frame width: {doc.width}")
        canvas.saveState()
        canvas.setFont('Helvetica-Bold', 16)
        canvas.drawCentredString(letter[0] / 2, letter[1] - 100, "IT3(t) Tax Certificate")
        canvas.restoreState()

    def second_page(canvas, doc):
        logging.debug(f"Rendering second page, frame height: {doc.height}, frame width: {doc.width}")
        canvas.saveState()
        canvas.setFont('Helvetica', 12)
        canvas.drawCentredString(letter[0] / 2, letter[1] / 2, "I3T")
        canvas.restoreState()

    def later_pages(canvas, doc):
        logging.debug(f"Rendering later page {doc.page}, frame height: {doc.height}, frame width: {doc.width}")
        canvas.saveState()
        canvas.setFont('Helvetica', 9)
        canvas.drawString(30, 30, f"Page {doc.page}")
        canvas.restoreState()

    # Story for all pages
    story = []

    # Page 1: Title
    logging.debug(f"Adding Spacer(1, 100) for page 1")
    story.append(Spacer(1, 100))  # Reduced from 642 to avoid large flowable
    story.append(PageBreak())
    logging.debug("Added PageBreak for page 1")

    # Page 2: Code (no flowables, only canvas drawing)
    story.append(PageBreak())
    logging.debug("Added PageBreak for page 2")

    # Page 3: Tax Certificate Information
    logging.debug("Adding content for page 3")
    story.append(Paragraph("Tax Certificate Information", styles['Heading1']))
    story.append(Spacer(1, 12))
    story.append(Paragraph(f"Issued by: {trust['TrustName']}", styles['Normal']))
    story.append(Paragraph(f"Trust Registration Number: {trust['TrustRegNumber']}", styles['Normal']))
    story.append(Paragraph(f"Tax Number: {trust['TaxNumber'] or 'N/A'}", styles['Normal']))
    story.append(Paragraph(f"Tax Year: 2025 (2024-03-01 to 2025-02-28)", styles['Normal']))
    story.append(Paragraph(f"Submission Date: {datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}", styles['Normal']))
    story.append(Spacer(1, 12))

    beneficiary = next((b for b in beneficiaries if b['BeneficiaryID'] == beneficiary_id), None)
    if beneficiary:
        story.append(Paragraph("Taxpayer Details:", styles['Heading2']))
        story.append(Paragraph(f"- Name: {beneficiary['LastName']}, {beneficiary['FirstName']}", styles['Normal']))
        story.append(
            Paragraph(f"- Tax Reference Number: {beneficiary['TaxReferenceNumber'] or 'N/A'}", styles['Normal']))
        story.append(
            Paragraph(f"- ID Number/Date of Birth: {beneficiary['IDNumber'] or beneficiary['DateOfBirth'] or 'N/A'}",
                      styles['Normal']))
        story.append(Spacer(1, 12))

        story.append(Paragraph("Transaction Details:", styles['Heading2']))
        tad_txs = [t for t in transactions if
                   t['BeneficiaryID'] == beneficiary_id and t['TransactionType'].startswith('TAD')]
        if tad_txs:
            story.append(Paragraph("Taxable Distributions (TAD):", styles['Normal']))
            for tx in tad_txs:
                story.append(
                    Paragraph(f"- Code: {tx['TransactionType'].replace('TAD_', '')}, Amount: {tx['Amount']:.2f}",
                              styles['Normal']))
        tff_txs = [t for t in transactions if
                   t['BeneficiaryID'] == beneficiary_id and t['TransactionType'].startswith('TFF')]
        if tff_txs:
            story.append(Paragraph("Financial Flows (TFF):", styles['Normal']))
            for tx in tff_txs:
                story.append(
                    Paragraph(f"- Type: {tx['TransactionType'].replace('TFF_', '')}, Amount: {tx['Amount']:.2f}",
                              styles['Normal']))
        total_amount = sum(float(tx['Amount']) for tx in tad_txs + tff_txs)
        story.append(Paragraph(f"- Total Amount: {total_amount:.2f}", styles['Normal']))
        story.append(Spacer(1, 12))

        story.append(Paragraph("Declaration:", styles['Heading2']))
        story.append(Paragraph(
            "This is to certify that the above details are true and correct as per the IT3(t) submission for the 2025 tax year.",
            styles['Normal']))
        story.append(
            Paragraph(f"Unique Record ID: {beneficiary['UniqueRecordID'] or str(uuid.uuid4())}", styles['Normal']))

    # Log flowable heights
    for i, flowable in enumerate(story):
        height = getattr(flowable, 'height', 0) + getattr(flowable, 'getSpaceAfter', lambda: 0)()
        logging.debug(f"Flowable {i}: {flowable.__class__.__name__}, Height: {height}")

    doc.build(story, onFirstPage=first_page, onLaterPages=later_pages)


def generate_file_content(trust_id, gh_unique_id):
    # CHANGED: Replaced direct connection with get_db_connection() to use Flask's g object
    conn = get_db_connection()
    cursor = conn.cursor()
    trust = cursor.execute('SELECT * FROM Trusts WHERE TrustID = ?', (trust_id,)).fetchone()
    if not trust:
        # CHANGED: Removed conn.close() here to avoid closing before raising the error
        raise ValueError('Trust not found')

    # Fetch submission data
    submission = cursor.execute('SELECT * FROM Submissions WHERE TrustID = ? ORDER BY SubmissionDate DESC LIMIT 1',
                                (trust_id,)).fetchone()
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

    # Fetch beneficiaries and transactions
    cursor.execute('SELECT * FROM Beneficiaries WHERE TrustID = ?', (trust_id,))
    beneficiaries = cursor.fetchall()
    dpb_count = len(beneficiaries)

    # 1. Sum TAD amounts
    cursor.execute(
        'SELECT * FROM BeneficiaryTAD WHERE TrustID = ?',
        (trust_id,))
    tad_records = cursor.fetchall()
    tad_count = len(tad_records)

    cursor.execute(
        'SELECT * FROM BeneficiaryTFF WHERE TrustID = ?',
        (trust_id,))
    tff_records = cursor.fetchall()
    tff_count = len(tff_records)

    # Generate file content
    lines = []
    sequence_number = 1
    ri_unique_id = str(uuid.uuid4())
    header_date = datetime.now()

    # HGH Record
    # gh_line = f"H|GH|{header_date.strftime('%Y-%m-%dT%H:%M:%S')}|1|{gh_unique_id}||L|I3T|HTTPS|9CD036C9-210F-40C5-91F7-82959AB269C02228AE14-53A7-4AC2-A539-A20D1D5654E6F4A52305-B91E-4435-B25E-46E38A528398D60DAB72-1C44-4490-99AF-FA572D3AFC69|GreatSoft|2024.3.1|Karin|Roux|0123428393|0606868076|0823714777|karin@rosspienaar.co.za"
    gh_line = f"H|GH|{header_date.strftime('%Y-%m-%dT%H:%M:%S')}|1|{gh_unique_id}||T|I3T|HTTPS|9CD036C9-210F-40C5-91F7-82959AB269C02228AE14-53A7-4AC2-A539-A20D1D5654E6F4A52305-B91E-4435-B25E-46E38A528398D60DAB72-1C44-4490-99AF-FA572D3AFC69|GreatSoft|2024.3.1|Karin|Roux|0123428393|0606868076|0823714777|karin@rosspienaar.co.za"
    lines.append(gh_line)

    # HSE Record (Submitting Entity)
    se_line = f"H|SE|{trust['SubmissionTaxYear']}|{trust['PeriodStartDate'] or '2024-03-01'}|{trust['PeriodEndDate'] or '2025-02-28'}|INDIVIDUAL|Pienaar|DJ|Daniel Jacobus||001|6307205099081|ZA|00212071|SAICA|1517179642||PO BOX 35336|MENLOPARK|||0102|0123428393|0606868076|tax@rosspienaar.co.za"
    lines.append(se_line)

    # RI Record
    # ri_line = f"B|RI|N|{ri_unique_id}|{sequence_number}|{trust['NatureOfPerson']}|{trust['TrustName']}|{trust['TrustRegNumber']}|{''}|{trust['TaxNumber'] or ''}|{'U'}|{trust['Residency'] or 'ZA'}|{trust['MastersOffice'] or ''}|{trust['TrustType'] or ''}|{trust['PhysicalUnitNumber'] or ''}|{trust['PhysicalComplex'] or ''}|{trust['PhysicalStreetNumber'] or ''}|{trust['PhysicalStreet']}|{trust['PhysicalSuburb'] or ''}|{trust['PhysicalCity']}|{trust['PhysicalPostalCode']}|{trust['PostalSameAsPhysical'] and 'Y' or 'N'}|{trust['PostalAddressLine1'] or ''}|{trust['PostalAddressLine2'] or ''}|{trust['PostalAddressLine3'] or ''}|{trust['PostalAddressLine4'] or ''}|{trust['PostalCode'] or ''}|{trust['ContactNumber'] or '999999999999999'}|{trust['CellNumber'] or '999999999999999'}|{trust['Email'] or ''}"
    ri_line = f"B|RI|N|{ri_unique_id}|{sequence_number}|{trust['NatureOfPerson']}|{trust['TrustName']}|{trust['TrustRegNumber']}|{trust['DateRegisteredMastersOffice'].replace('-', '')}|{trust['TaxNumber'] or ''}|{''}|{trust['Residency'] or 'ZA'}|{trust['MastersOffice'] or ''}|{trust['TrustType'] or ''}|{trust['PhysicalUnitNumber'] or ''}|{trust['PhysicalComplex'] or ''}|{trust['PhysicalStreetNumber'] or ''}|{trust['PhysicalStreet']}|{trust['PhysicalSuburb'] or ''}|{trust['PhysicalCity']}|{trust['PhysicalPostalCode']}|{trust['PostalSameAsPhysical'] and 'Y' or 'N'}|{trust['PostalAddressLine1'] or ''}|{trust['PostalAddressLine2'] or ''}|{trust['PostalAddressLine3'] or ''}|{trust['PostalAddressLine4'] or ''}|{trust['PostalCode'] or ''}|{trust['ContactNumber'] or '999999999999999'}|{trust['CellNumber'] or '999999999999999'}|{trust['Email'] or ''}"
    lines.append(ri_line)
    sequence_number += 1

    # DPB, TAD, TFF Records
    # Fetch beneficiaries and loop
    beneficiaries = conn.execute('SELECT * FROM Beneficiaries WHERE TrustID = ?', (trust_id,)).fetchall()

    for beneficiary in beneficiaries:
        dpb_unique_id = beneficiary['UniqueRecordID'] or str(uuid.uuid4())
        dpb_line = (f"B|DPB|N|{dpb_unique_id}|{sequence_number}|{ri_unique_id}"  # 401-406
                    f"|{beneficiary['IsConnectedPerson'] and 'Y' or 'N'}"  # 407
                    f"|{beneficiary['IsBeneficiary'] and 'Y' or 'N'}"  # 408
                    f"|{beneficiary['IsFounder'] and 'Y' or 'N'}"  # 409
                    f"|{beneficiary['NatureOfPerson'] == '1' and 'Y' or 'N'}"  # 410
                    f"|{beneficiary['IsDonor'] and 'Y' or 'N'}"  # 411
                    f"|{beneficiary['IsNonResident'] and 'Y' or 'N'}"  # 412
                    f"|{beneficiary['TaxReferenceNumber'] or ''}"  # 413
                    f"|{beneficiary['LastName']}"  # 414
                    f"|{beneficiary['FirstName']}"  # 415
                    f"|{beneficiary['OtherName'] or ''}"  # 416
                    f"|{beneficiary['Initials'] or ''}"  # 417
                    f"|{(beneficiary['DateOfBirth'] or '').replace('-', '')}"  # 418
                    f"|{beneficiary['IDNumber'] or ''}"  # 419
                    f"|{beneficiary['PassportNumber'] or ''}"  # 420
                    f"|{beneficiary['PassportCountry'] or ''}"  # 421
                    f"|{beneficiary['PassportIssueDate'] or ''}"  # 422
                    f"|{sanitize(beneficiary['CompanyIncomeTaxRefNo'])}"  # 423 
                    f"|{sanitize(beneficiary['CompanyRegistrationNumber'])}"  # 424
                    f"|{sanitize(beneficiary['CompanyRegisteredName'])}"  # 425
                    f"|{beneficiary['PhysicalUnitNumber'] or ''}"  # 426
                    f"|{beneficiary['PhysicalComplex'] or ''}"  # 427
                    f"|{beneficiary['PhysicalStreetNumber'] or ''}"  # 428
                    f"|{beneficiary['PhysicalStreet'] or ''}"  # 429
                    f"|{beneficiary['PhysicalSuburb'] or ''}"  # 430
                    f"|{beneficiary['PhysicalCity'] or ''}"  # 431
                    f"|{beneficiary['PhysicalPostalCode'] or ''}"  # 432
                    f"|{beneficiary['PostalSameAsPhysical'] and 'Y' or 'N'}"  # 433
                    f"|{beneficiary['PostalAddressLine1'] or ''}"  # 434
                    f"|{beneficiary['PostalAddressLine2'] or ''}"  # 435
                    f"|{beneficiary['PostalAddressLine3'] or ''}"  # 436
                    f"|{beneficiary['PostalAddressLine4'] or ''}"  # 437
                    f"|{beneficiary['PostalCode'] or ''}"  # 438
                    f"|{beneficiary['ContactNumber'] or '999999999999999'}"  # 439
                    f"|{beneficiary['CellNumber'] or '999999999999999'}"  # 440
                    f"|{beneficiary['Email'] or ''}"  # 441
                    f"|{beneficiary['IsTaxableOnDistributed'] and 'Y' or 'N'}"  # 442
                    f"|{beneficiary['HasNonTaxableAmounts'] and 'Y' or 'N'}"  # 443
                    f"|{beneficiary['HasCapitalDistribution'] and 'Y' or 'N'}"  # 444
                    f"|{beneficiary['MadeDonations'] and 'Y' or 'N'}"  # 447
                    f"|{beneficiary['MadeContributions'] and 'Y' or 'N'}"  # 448
                    f"|{beneficiary['ReceivedDonations'] and 'Y' or 'N'}"  # 449
                    f"|{beneficiary['ReceivedContributions'] and 'Y' or 'N'}"  # 450
                    f"|{beneficiary['MadeDistributions'] and 'Y' or 'N'}"  # 451
                    f"|{beneficiary['ReceivedRefunds'] and 'Y' or 'N'}"  # 452
                    f"|{beneficiary['HasRightOfUse'] and 'Y' or 'N'}"  # 453
                    )
        lines.append(dpb_line)
        sequence_number += 1

        # TAD Records
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM BeneficiaryTAD WHERE BeneficiaryID = ?',
                       (beneficiary['BeneficiaryID'],))
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

        # Fetch DNT transactions
        dnt_row = conn.execute(
            'SELECT * FROM BeneficiaryDNT WHERE BeneficiaryID = ?',
            (beneficiary['BeneficiaryID'],)
        ).fetchone()

        dnt_data = dict(dnt_row) if dnt_row else {
            'LocalDividends': 0.00,
            'ExemptForeignDividends': 0.00,
            'OtherNonTaxableIncome': 0.00
        }

        # Strip decimals before formatting
        for key in ['LocalDividends', 'ExemptForeignDividends', 'OtherNonTaxableIncome']:
            try:
                dnt_data[key] = int(float(dnt_data[key]))
            except (ValueError, TypeError):
                dnt_data[key] = 0

        print("HasNonTaxableAmounts =", beneficiary['HasNonTaxableAmounts'])

        if beneficiary['HasNonTaxableAmounts'] == 1:
            print('Has Non Taxable Amounts:')
            print("LocalDividends =", dnt_data['LocalDividends'])
            print("ExemptForeignDividends =", dnt_data['ExemptForeignDividends'])
            print("OtherNonTaxableIncome =", dnt_data['OtherNonTaxableIncome'])

            dnt_line = (
                f"B|DNT|N|{str(uuid.uuid4())}|{sequence_number}|{dpb_unique_id}|"
                f"{dnt_data['LocalDividends']}|{dnt_data['ExemptForeignDividends']}|{dnt_data['OtherNonTaxableIncome']}"
            )
            lines.append(dnt_line)

            print("lines.append(dnt_line)")
            sequence_number += 1

        # Fetch TFF transactions
        tff_row = conn.execute('SELECT * FROM BeneficiaryTFF WHERE BeneficiaryID = ?',
                               (beneficiary['BeneficiaryID'],)
                               ).fetchall()

        totalValueOfCapitalDistributed = int(sum(float(row['TotalValueOfCapitalDistributed']) for row in tff_row))
        totalExpensesIncurred = int(sum(float(row['TotalExpensesIncurred']) for row in tff_row))
        totalDonationsToTrust = int(sum(float(row['TotalDonationsToTrust']) for row in tff_row))
        totalContributionsToTrust = int(sum(float(row['TotalContributionsToTrust']) for row in tff_row))
        totalDonationsReceivedFromTrust = int(sum(float(row['TotalDonationsReceivedFromTrust']) for row in tff_row))
        totalContributionsReceivedFromTrust = int(
            sum(float(row['TotalContributionsReceivedFromTrust']) for row in tff_row))
        totalDistributionsToTrust = int(sum(float(row['TotalDistributionsToTrust']) for row in tff_row))
        totalContributionsRefundedByTrust = int(sum(float(row['TotalContributionsRefundedByTrust']) for row in tff_row))

        # Construct TFF line using spec fields 707–714
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

    file_body = ''.join(lines)  # lines = all lines except the trailer
    print("file_body before hash\n", file_body)

    # 2. Calculate the hash
    md5_hash = hashlib.md5(file_body.encode('utf-8')).hexdigest()
    print("md5_hash=", md5_hash)

    # 3. Build and append the trailer line
    dnt_row = conn.execute(
        'SELECT * FROM BeneficiaryDNT WHERE TrustID = ?',
        (trust_id,)
    ).fetchall()

    tff_row = conn.execute('SELECT * FROM BeneficiaryTFF WHERE TrustID = ?',
                           (trust_id,)
                           ).fetchall()

    totalValueOfCapitalDistributed = int(sum(float(row['TotalValueOfCapitalDistributed']) for row in tff_row))
    totalExpensesIncurred = int(sum(float(row['TotalExpensesIncurred']) for row in tff_row))
    totalDonationsToTrust = int(sum(float(row['TotalDonationsToTrust']) for row in tff_row))
    totalContributionsToTrust = int(sum(float(row['TotalContributionsToTrust']) for row in tff_row))
    totalDonationsReceivedFromTrust = int(sum(float(row['TotalDonationsReceivedFromTrust']) for row in tff_row))
    totalContributionsReceivedFromTrust = int(sum(float(row['TotalContributionsReceivedFromTrust']) for row in tff_row))
    totalDistributionsToTrust = int(sum(float(row['TotalDistributionsToTrust']) for row in tff_row))
    totalContributionsRefundedByTrust = int(sum(float(row['TotalContributionsRefundedByTrust']) for row in tff_row))

    print("totalDistributionsToTrust = ", totalDistributionsToTrust)

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
    print("file_body after hash\n", lines)

    # Generate filename
    timestamp = header_date.strftime('%Y%m%dT%H%M%S')
    filename = f"I3T_1_1517179642_{gh_unique_id}_{timestamp}_B2BSFG.txt"

    # CHANGED: Removed conn.close() to prevent closing the connection prematurely
    return filename, '\n'.join(lines), trust, dpb_count, tad_count, tff_count, total_amount


@app.route('/generate_i3t_direct/<int:trust_id>')
def generate_i3t_direct(trust_id):
    try:
        gh_unique_id = str(uuid.uuid4())
        filename, file_content, trust, dpb_count, tad_count, tff_count, total_amount = generate_file_content(trust_id,
                                                                                                             gh_unique_id)
        submission_tax_year = trust['SubmissionTaxYear'] or datetime.now().strftime('%Y')
        folder = os.path.join(os.getcwd(),'IT3(t)', submission_tax_year, trust['TrustName'].replace(' ', '_'))



        os.makedirs(folder, exist_ok=True)

        # Save flatfile
        sars_file_path = os.path.join(folder, filename)
        with open(sars_file_path, 'w', encoding='ISO-8859-1') as f:
            f.write(file_content)

        # Parse flatfile to extract DPB and transaction data
        dpb_map = {}
        transactions = {}
        with open(sars_file_path, 'r', encoding='ISO-8859-1') as f:
            for line in f:
                parts = line.strip().split('|')
                if parts[0] == 'B' and parts[1] == 'DPB':
                    uid = parts[3]
                    dpb_map[uid] = {
                        'LastName': parts[10],
                        'FirstName': parts[11],
                        'TaxReferenceNumber': parts[9],
                        'IDNumber': parts[14],
                        'DateOfBirth': parts[13],
                        'UniqueRecordID': uid
                    }
                    transactions[uid] = {'TAD': [], 'TFF': []}
                elif parts[0] == 'B' and parts[1] == 'TAD':
                    uid = parts[3]
                    if uid not in transactions:
                        transactions[uid] = {'TAD': [], 'TFF': []}
                    transactions[uid]['TAD'].append({
                        'Code': parts[5],
                        'Amount': float(parts[4]),
                        'ForeignTaxCredits': float(parts[6])
                    })
                elif parts[0] == 'B' and parts[1] == 'TFF':
                    uid = parts[3]
                    if uid not in transactions:
                        transactions[uid] = {'TAD': [], 'TFF': []}
                    transactions[uid]['TFF'].append({
                        'Type': 'TFF',
                        'Values': parts[6:]
                    })

        # Update Trust record
        conn = get_db_connection()
        try:
            conn.execute("""
                UPDATE Trusts SET RecordStatus = ?
                WHERE TrustID = ?
            """, ('9001 - Submitted to SARS', trust_id))
            conn.commit()
            logging.info(f"Trust {trust_id} marked as '9001 - Submitted to SARS'")
        except sqlite3.Error as e:
            conn.rollback()
            logging.error(f"Database error updating Trust {trust_id}: {str(e)}")
            flash(f"Database error: {str(e)}", 'error')
            return render_template('i3t_summary.html', trust=trust, filename=filename, dpb_count=dpb_count,
                                   tad_count=tad_count, tff_count=tff_count, total_amount=total_amount,
                                   file_content=file_content, folder=folder)

        # Generate individual PDFs using parsed data
        for uid, data in dpb_map.items():
            pdf_filename = f"{data['LastName']}_{data['FirstName']}.pdf"
            pdf_path = os.path.join(folder, pdf_filename)
            pdf_buffer = io.BytesIO()
            generate_it3t_pdf_from_flatfile(data, dict(trust), transactions.get(uid, {}), pdf_buffer)
            with open(pdf_path, 'wb') as f:
                f.write(pdf_buffer.getvalue())

        flash(f"SARS flatfile and individual IT3(t) PDFs generated in {folder}", 'success')
        return render_template('i3t_summary.html', trust=trust, filename=filename, dpb_count=dpb_count,
                               tad_count=tad_count, tff_count=tff_count, total_amount=total_amount,
                               file_content=file_content, folder=folder)

    except ValueError as e:
        logging.error(f"ValueError in generate_i3t_direct: {str(e)}")
        return str(e), 404
    except Exception as e:
        logging.error(f"Unexpected error in generate_i3t_direct: {str(e)}")
        return f"Error generating SARS artefacts: {str(e)}", 500


@app.route('/generate_sars_file/<int:trust_id>')
def generate_sars_file(trust_id):
    try:
        gh_unique_id = request.args.get('gh_unique_id')
        if not gh_unique_id:
            flash('Proposed Unique ID is required to generate SARS file!')
            return redirect(url_for('generate_i3t', trust_id=trust_id))

        # Step 1: Generate flatfile
        filename, file_content, trust, _, _, _, _, _, _ = generate_file_content(trust_id, gh_unique_id)
        submission_tax_year = trust['SubmissionTaxYear'] or datetime.now().strftime('%Y')
        folder = os.path.join('IT3(t)', submission_tax_year, trust['TrustName'].replace(' ', '_'))
        os.makedirs(folder, exist_ok=True)
        sars_file_path = os.path.join(folder, filename)
        with open(sars_file_path, 'w', encoding='ISO-8859-1', newline='\n') as f:
            f.write(file_content)

        # Step 2: Parse flatfile and generate PDFs
        with open(sars_file_path, 'r', encoding='ISO-8859-1') as f:
            lines = f.readlines()

        dpb_map = {}
        transactions = {}

        for line in lines:
            parts = line.strip().split('|')
            if parts[0] == 'B' and parts[1] == 'DPB':
                uid = parts[3]
                dpb_map[uid] = {
                    'LastName': parts[10],
                    'FirstName': parts[11],
                    'TaxReferenceNumber': parts[9],
                    'IDNumber': parts[14],
                    'DateOfBirth': parts[13],
                    'UniqueRecordID': uid
                }
                transactions[uid] = {'TAD': [], 'TFF': []}
            elif parts[0] == 'B' and parts[1] == 'TAD':
                transactions[parts[3]]['TAD'].append({
                    'Code': parts[5],
                    'Amount': float(parts[4]),
                    'ForeignTaxCredits': float(parts[6])
                })
            elif parts[0] == 'B' and parts[1] == 'TFF':
                transactions[parts[3]]['TFF'].append({
                    'Type': 'TFF',
                    'Values': parts[6:]
                })

        for uid, data in dpb_map.items():
            pdf_filename = f"{data['LastName']}_{data['FirstName']}.pdf"
            pdf_path = os.path.join(folder, pdf_filename)
            pdf_buffer = io.BytesIO()
            generate_it3t_pdf_from_flatfile(data, dict(trust), transactions.get(uid, {}), pdf_buffer)
            with open(pdf_path, 'wb') as f:
                f.write(pdf_buffer.getvalue())

        flash(f"SARS flatfile and individual IT3(t) PDFs generated in {folder}", 'success')
        return redirect(url_for('generate_i3t', trust_id=trust_id, gh_unique_id=gh_unique_id))

    except Exception as e:
        return f"Error generating SARS artefacts: {str(e)}", 500


@app.route('/generate_individual_it3t/<int:trust_id>')
def generate_individual_it3t(trust_id):
    try:
        gh_unique_id = request.args.get('gh_unique_id')
        if not gh_unique_id:
            flash('Proposed Unique ID is required to generate individual IT3(t)s!')
            return redirect(url_for('generate_i3t', trust_id=trust_id))
        filename, file_content, trust, beneficiaries, transactions, dpb_count, tad_count, tff_count, total_amount = generate_file_content(
            trust_id, gh_unique_id)

        # Create subfolder based on SubmissionTaxYear
        submission_tax_year = trust['SubmissionTaxYear'] or datetime.now().strftime('%Y')
        folder = os.path.join('IT3(t)', submission_tax_year, trust['TrustName'].replace(' ', '_'))

        os.makedirs(folder, exist_ok=True)

        # Generate individual PDFs
        for beneficiary in beneficiaries:
            pdf_filename = f"{beneficiary['LastName']}_{beneficiary['FirstName']}.pdf"
            pdf_path = os.path.join(folder, pdf_filename)
            pdf_buffer = io.BytesIO()
            generate_it3t_pdf(beneficiary['BeneficiaryID'], dict(trust), beneficiaries, transactions, pdf_buffer)
            with open(pdf_path, 'wb') as f:
                f.write(pdf_buffer.getvalue())

        # Redirect back to generate_i3t with success message
        flash(f"Individual IT3(t) PDFs generated in {folder}", 'success')
        return redirect(url_for('generate_i3t', trust_id=trust_id, gh_unique_id=gh_unique_id))
    except ValueError as e:
        return str(e), 404
    except Exception as e:
        return f"Error generating individual IT3(t)s: {str(e)}", 500


def none_to_blank(value):
    return '' if value in (None, 'None', 'null', 'none', '<null>') else value


# Add these routes to app.py

@app.route('/export_data', methods=['GET'])
def export_data():
    conn = get_db_connection()
    tables = ['Beneficiaries', 'BeneficiaryDNT', 'BeneficiaryTAD', 'BeneficiaryTFF', 'HGHHeaders', 'Submissions',
              'Trusts']
    data = {}
    for table in tables:
        cursor = conn.execute(f'SELECT * FROM {table}')
        data[table] = [dict(row) for row in cursor.fetchall()]
    conn.close()

    # Create JSON response
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

        # Drop tables
        tables = ['Beneficiaries', 'BeneficiaryDNT', 'BeneficiaryTAD', 'BeneficiaryTFF', 'FinancialTransactions',
                  'HGHHeaders', 'Submissions', 'Trusts']
        for table in tables:
            cursor.execute(f'DROP TABLE IF EXISTS {table}')

        # Recreate tables (inferred schemas)
        cursor.execute('''
            CREATE TABLE Trusts (
                TrustID INTEGER PRIMARY KEY,
                TrustRegNumber TEXT,
                TrustName TEXT,
                TaxNumber TEXT,
                SubmissionTaxYear TEXT,
                PeriodStartDate TEXT,
                PeriodEndDate TEXT,
                NatureOfPerson TEXT,
                TrustType TEXT,
                Residency TEXT,
                MastersOffice TEXT,
                DateRegisteredMastersOffice TEXT,
                PhysicalUnitNumber TEXT,
                PhysicalComplex TEXT,
                PhysicalStreetNumber TEXT,
                PhysicalStreet TEXT,
                PhysicalSuburb TEXT,
                PhysicalCity TEXT,
                PhysicalPostalCode TEXT,
                PostalSameAsPhysical INTEGER,
                PostalAddressLine1 TEXT,
                PostalAddressLine2 TEXT,
                PostalAddressLine3 TEXT,
                PostalAddressLine4 TEXT,
                PostalCode TEXT,
                ContactNumber TEXT,
                CellNumber TEXT,
                Email TEXT,
                RecordStatus TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE Beneficiaries (
                BeneficiaryID INTEGER PRIMARY KEY,
                TrustID INTEGER,
                TaxReferenceNumber TEXT,
                IsNaturalPerson INTEGER,
                LastName TEXT,
                FirstName TEXT,
                OtherName TEXT,
                Initials TEXT,
                DateOfBirth TEXT,
                IdentificationType TEXT,
                IDNumber TEXT,
                PassportNumber TEXT,
                PassportCountry TEXT,
                PassportIssueDate TEXT,
                CompanyIncomeTaxRefNo TEXT,
                CompanyRegistrationNumber TEXT,
                CompanyRegisteredName TEXT,
                NatureOfPerson TEXT,
                IsConnectedPerson INTEGER,
                IsBeneficiary INTEGER,
                IsFounder INTEGER,
                IsDonor INTEGER,
                IsNonResident INTEGER,
                IsTaxableOnDistributed INTEGER,
                HasNonTaxableAmounts INTEGER,
                HasCapitalDistribution INTEGER,
                MadeDonations INTEGER,
                MadeContributions INTEGER,
                ReceivedDonations INTEGER,
                ReceivedContributions INTEGER,
                MadeDistributions INTEGER,
                ReceivedRefunds INTEGER,
                HasRightOfUse INTEGER,
                PhysicalUnitNumber TEXT,
                PhysicalComplex TEXT,
                PhysicalStreetNumber TEXT,
                PhysicalStreet TEXT,
                PhysicalSuburb TEXT,
                PhysicalCity TEXT,
                PhysicalPostalCode TEXT,
                PostalSameAsPhysical INTEGER,
                PostalAddressLine1 TEXT,
                PostalAddressLine2 TEXT,
                PostalAddressLine3 TEXT,
                PostalAddressLine4 TEXT,
                PostalCode TEXT,
                ContactNumber TEXT,
                CellNumber TEXT,
                Email TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE BeneficiaryDNT (
                BeneficiaryID INTEGER PRIMARY KEY,
                LocalDividends REAL,
                ExemptForeignDividends REAL,
                OtherNonTaxableIncome REAL
            )
        ''')

        cursor.execute('''
            CREATE TABLE BeneficiaryTAD (
                TADID INTEGER PRIMARY KEY,
                BeneficiaryID INTEGER,
                SourceCode TEXT,
                AmountSubjectToTax REAL,
                ForeignTaxCredits REAL
            )
        ''')

        cursor.execute('''
            CREATE TABLE BeneficiaryTFF (
                BeneficiaryID INTEGER PRIMARY KEY,
                TotalValueOfCapitalDistributed REAL,
                TotalExpensesIncurred REAL,
                TotalDonationsToTrust REAL,
                TotalContributionsToTrust REAL,
                TotalDonationsReceivedFromTrust REAL,
                TotalContributionsReceivedFromTrust REAL,
                TotalDistributionsToTrust REAL,
                TotalContributionsRefundedByTrust REAL
            )
        ''')

        cursor.execute('''
            CREATE TABLE FinancialTransactions (
                TransactionID INTEGER PRIMARY KEY,
                TrustID INTEGER,
                BeneficiaryID INTEGER,
                TransactionType TEXT,
                Amount REAL,
                Date TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE HGHHeaders (
                HGHID INTEGER PRIMARY KEY,
                SectionIdentifier TEXT,
                HeaderType TEXT,
                MessageCreateDate TEXT,
                FileLayoutVersion TEXT,
                UniqueFileID TEXT,
                SARSRequestReference TEXT,
                TestDataIndicator TEXT,
                DataTypeSupplied TEXT,
                ChannelIdentifier TEXT,
                SourceIdentifier TEXT,
                SourceSystem TEXT,
                SourceSystemVersion TEXT,
                ContactPersonName TEXT,
                ContactPersonSurname TEXT,
                BusinessTelephoneNumber1 TEXT,
                BusinessTelephoneNumber2 TEXT,
                CellPhoneNumber TEXT,
                ContactEmail TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE Submissions (
                SubmissionID INTEGER PRIMARY KEY,
                TrustID INTEGER,
                SubmissionDate TEXT,
                Status TEXT
            )
        ''')

        # Insert data
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
from flask import render_template, request, flash, redirect, url_for


# Ensure this is part of your existing Flask app setup
# ... (other existing routes and imports)

@app.route('/kill_process_tree')
def kill_process_tree():
    try:
        # Execute taskkill command to terminate the RossPienaar_IT3(t).exe process tree
        # /IM specifies the image name, /T terminates the tree, /F forces the termination
        result = subprocess.run(
            ['taskkill', '/IM', 'RossPienaar_IT3(t).exe', '/T', '/F'],
            capture_output=True,
            text=True,
            check=True
        )
        flash('Process tree killed successfully: ' + result.stdout, 'success')
        print('Process tree killed successfully: ' + result.stdout, 'success')
    except subprocess.CalledProcessError as e:
        flash(f'Failed to kill process: {e.stderr}', 'error')
    except Exception as e:
        flash(f'Error: {str(e)}', 'error')

    # Redirect back to the index page
    return redirect(url_for('index'))


# Register the filter with Jinja2
app.jinja_env.filters['none_to_blank'] = none_to_blank

if __name__ == '__main__':
    threading.Timer(1.25, lambda: webbrowser.open('http://127.0.0.1:5000')).start()  # Adjust port if needed
    app.run(debug=True)
