-- Trusts table to store trust demographic information
CREATE TABLE Trusts (
    TrustID INTEGER PRIMARY KEY AUTOINCREMENT,
    TrustRegNumber TEXT NOT NULL UNIQUE,
    TrustName TEXT NOT NULL,
    TaxNumber TEXT,
    Address TEXT,
    ContactNumber TEXT,
    Email TEXT
);

-- Trustees table to store trustee details
CREATE TABLE Trustees (
    TrusteeID INTEGER PRIMARY KEY AUTOINCREMENT,
    TrustID INTEGER NOT NULL,
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    IDNumber TEXT NOT NULL,
    ContactNumber TEXT,
    Email TEXT,
    IsMainTrustee BOOLEAN NOT NULL DEFAULT 0,
    FOREIGN KEY (TrustID) REFERENCES Trusts(TrustID)
);

-- Beneficiaries table to store beneficiary details
CREATE TABLE Beneficiaries (
    BeneficiaryID INTEGER PRIMARY KEY AUTOINCREMENT,
    IDNumber TEXT NOT NULL UNIQUE,
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    Address TEXT,
    ContactNumber TEXT,
    Email TEXT
);

-- FinancialTransactions table to store financial data
CREATE TABLE FinancialTransactions (
    TransactionID INTEGER PRIMARY KEY AUTOINCREMENT,
    TrustID INTEGER NOT NULL,
    BeneficiaryID INTEGER NOT NULL,
    TransactionType TEXT NOT NULL,
    Amount DECIMAL(15, 2) NOT NULL,
    IsTaxable BOOLEAN NOT NULL DEFAULT 0,
    TransactionDate TEXT NOT NULL,
    FOREIGN KEY (TrustID) REFERENCES Trusts(TrustID),
    FOREIGN KEY (BeneficiaryID) REFERENCES Beneficiaries(BeneficiaryID)
);

-- Submissions table to track SARS submissions and responses
CREATE TABLE Submissions (
    SubmissionID INTEGER PRIMARY KEY AUTOINCREMENT,
    TrustID INTEGER NOT NULL,
    SubmissionDate TEXT NOT NULL,
    FilePath TEXT NOT NULL,
    Status TEXT NOT NULL,
    ResponseDetails TEXT,
    FOREIGN KEY (TrustID) REFERENCES Trusts(TrustID)
);