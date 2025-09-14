CREATE TABLE Beneficiaries
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
   RecordStatus varchar(50)
)
;
ALTER TABLE Beneficiaries
ADD CONSTRAINT 
FOREIGN KEY (TrustID)
REFERENCES Trusts(TrustID) ON DELETE CASCADE
;
CREATE TABLE BeneficiaryDNT
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
)
;
CREATE TABLE BeneficiaryTAD
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
)
;
ALTER TABLE BeneficiaryTAD
ADD CONSTRAINT 
FOREIGN KEY (BeneficiaryID)
REFERENCES Beneficiaries(BeneficiaryID)
;
CREATE TABLE BeneficiaryTFF
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
)
;
CREATE TABLE HGHHeaders
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
)
;
CREATE TABLE Submissions
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
)
;
ALTER TABLE Submissions
ADD CONSTRAINT 
FOREIGN KEY (TrustID)
REFERENCES Trusts(TrustID) ON DELETE CASCADE
;
CREATE TABLE Trusts
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
)
;
CREATE UNIQUE INDEX sqlite_autoindex_Trusts_1 ON Trusts(TrustRegNumber)
;
