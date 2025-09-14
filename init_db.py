import sqlite3

def init_db():
    conn = sqlite3.connect('it3t.db')
    cursor = conn.cursor()

    # Create Trusts table with updated fields including NatureOfPerson
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Trusts (
            TrustID INTEGER PRIMARY KEY AUTOINCREMENT,
            TrustRegNumber TEXT NOT NULL UNIQUE,
            TrustName TEXT NOT NULL,
            TaxNumber TEXT,
            Status TEXT,
            ReportingDate DATE,
            NatureOfPerson TEXT,
            TrustType TEXT,
            Residency TEXT,
            MastersOffice TEXT,
            PhysicalUnitNumber TEXT,
            PhysicalComplex TEXT,
            PhysicalStreetNumber TEXT,
            PhysicalStreet TEXT,
            PhysicalSuburb TEXT,
            PhysicalCity TEXT,
            PhysicalPostalCode TEXT,
            PostalSameAsPhysical BOOLEAN,
            PostalAddressLine1 TEXT,
            PostalAddressLine2 TEXT,
            PostalAddressLine3 TEXT,
            PostalAddressLine4 TEXT,
            PostalCode TEXT,
            ContactNumber TEXT,
            CellNumber TEXT,
            Email TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Beneficiaries (
            BeneficiaryID INTEGER PRIMARY KEY AUTOINCREMENT,
            TrustID INTEGER NOT NULL,
            TaxReferenceNumber TEXT,
            LastName TEXT NOT NULL,
            FirstName TEXT NOT NULL,
            OtherName TEXT,
            Initials TEXT,
            DateOfBirth DATE,
            IDNumber TEXT,
            IdentificationType TEXT NOT NULL DEFAULT '001',
            PassportNumber TEXT,
            PassportCountry TEXT,
            PassportIssueDate DATE,
            CompanyRegistrationNumber TEXT,
            CompanyRegisteredName TEXT,
            NatureOfPerson TEXT,
            IsConnectedPerson BOOLEAN,
            IsBeneficiary BOOLEAN,
            IsFounder BOOLEAN,
            IsNaturalPerson BOOLEAN,
            IsDonor BOOLEAN,
            IsNonResident BOOLEAN,
            IsTaxableOnDistributed BOOLEAN,
            HasNonTaxableAmounts BOOLEAN,
            HasCapitalDistribution BOOLEAN,
            HasLoansGranted BOOLEAN,
            HasLoansFrom BOOLEAN,
            MadeDonations BOOLEAN,
            MadeContributions BOOLEAN,
            ReceivedDonations BOOLEAN,
            ReceivedContributions BOOLEAN,
            MadeDistributions BOOLEAN,
            ReceivedRefunds BOOLEAN,
            HasRightOfUse BOOLEAN,
            PhysicalUnitNumber TEXT,
            PhysicalComplex TEXT,
            PhysicalStreetNumber TEXT,
            PhysicalStreet TEXT,
            PhysicalSuburb TEXT,
            PhysicalCity TEXT,
            PhysicalPostalCode TEXT,
            PostalSameAsPhysical BOOLEAN,
            PostalAddressLine1 TEXT,
            PostalAddressLine2 TEXT,
            PostalAddressLine3 TEXT,
            PostalAddressLine4 TEXT,
            PostalCode TEXT,
            ContactNumber TEXT,
            CellNumber TEXT,
            Email TEXT,
            FOREIGN KEY (TrustID) REFERENCES Trusts(TrustID) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS FinancialTransactions (
            TransactionID INTEGER PRIMARY KEY AUTOINCREMENT,
            TrustID INTEGER NOT NULL,
            BeneficiaryID INTEGER NOT NULL,
            TransactionType TEXT NOT NULL,
            TaxableAmount DECIMAL(15, 2) DEFAULT 0.00,
            NonTaxableAmount DECIMAL(15, 2) DEFAULT 0.00,
            DistributedAmount DECIMAL(15, 2) DEFAULT 0.00,
            VestedAmount DECIMAL(15, 2) DEFAULT 0.00,
            TransactionDate DATE NOT NULL,
            FOREIGN KEY (TrustID) REFERENCES Trusts(TrustID) ON DELETE CASCADE,
            FOREIGN KEY (BeneficiaryID) REFERENCES Beneficiaries(BeneficiaryID) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Submissions (
            SubmissionID INTEGER PRIMARY KEY AUTOINCREMENT,
            TrustID INTEGER NOT NULL,
            TaxRefNo TEXT NOT NULL,
            UniqueFileID TEXT NOT NULL,
            SubmissionDate DATE NOT NULL,
            FilePath TEXT NOT NULL,
            Status TEXT NOT NULL,
            FileResponseCode TEXT,
            ResponseDetails TEXT,
            FOREIGN KEY (TrustID) REFERENCES Trusts(TrustID) ON DELETE CASCADE
        )
    """)

    conn.commit()
    conn.close()