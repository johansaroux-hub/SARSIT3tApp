# SARSIT3tApp by JDLSoft

A Flask-based web application by JDLSoft for tax practitioners to streamline SARS IT3(t) submissions, manage trust data, and generate compliance files and PDFs.

## Features
- Manage trusts, beneficiaries, and submissions via a web interface.
- Validate South African ID and tax numbers using modulus-10 and date-of-birth checks.
- Generate SARS-compliant IT3(t) flat files and individual PDF certificates.
- Store data in a SQLite database (`beneficial_ownership.db`).

## Setup
1. Clone the repository: `git clone https://github.com/yourusername/SARSIT3tApp.git`
2. Create a virtual environment: `python -m venv venv`
3. Activate the virtual environment:
   - Windows: `venv\Scripts\activate`
   - macOS/Linux: `source venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`
5. Run the app: `python app.py`
6. Access at `http://127.0.0.1:5000`

## Project Structure
- `app.py`: Main Flask application.
- `requirements.txt`: Python dependencies for JDLSoft SARS IT3(t) app.
- `templates/`: HTML templates for the web interface.
- `static/`: CSS and JavaScript files.
- `schema.sql`: SQLite database schema (for `beneficial_ownership.db`).

## Notes
- Set `FLASK_SECRET_KEY` in a `.env` file for production.
- Generated files are saved in `IT3(t)/<TaxYear>/<TrustName>/`.
- For Azure deployment, migrate SQLite to Azure SQL Database (see `pyodbc` in requirements).

## License
MIT License (optional)