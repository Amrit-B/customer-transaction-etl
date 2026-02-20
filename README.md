# Customer Transaction ETL Pipeline

A production-style Java ETL (Extract, Transform, Load) pipeline for processing financial transaction data — built to mirror real-world data engineering workflows like those used in AML/fraud detection and identity verification platforms.

## What It Does

Processes raw customer transaction CSV data through a full ETL pipeline:

1. **Extract** — Reads CSV with robust parsing (quoted fields, multiple date formats, encoding issues)
2. **Clean** — Validates records, rejects unrecoverable rows, normalizes phone/name/email/currency
3. **Transform** — Applies business rules: currency conversion to USD, AML flagging, duplicate detection
4. **Load** — Batch inserts into SQLite with schema creation and verification queries
5. **Report** — Generates a data quality summary (console + file)

## AML Flagging Rules

| Rule | Condition |
|---|---|
| `LARGE_CASH_TRANSACTION` | Cash transaction ≥ $10,000 USD (CTR threshold) |
| `POTENTIAL_STRUCTURING` | Cash transaction between $9,000–$9,999 USD |
| `HIGH_RISK_COUNTRY` | Transaction from FATF high-risk jurisdictions |
| `LARGE_WIRE_TRANSFER` | Wire transfer > $50,000 USD |

## Sample Output

```
+----------------------------------------------+
|   Customer Transaction ETL Pipeline v1.0    |
+----------------------------------------------+
  Input  : data/transactions.csv
  Output : data/transactions.db

[Stage 1/4] Extracting data from CSV...
[CSVReader] Read 23 records. Skipped 0 malformed rows.

[Stage 2/4] Cleaning and validating records...
[DataCleaner] Cleaned: 20 | Rejected: 3

[Stage 3/4] Applying business transformations...
[Transformer] Duplicate transaction ID found and removed: TXN013
[Transformer] Transformed: 19 | Flagged: 8 | Duplicates removed: 1 | Currency conversions: 7

[Stage 4/4] Loading to database...
[DatabaseLoader] Schema initialized.
[DatabaseLoader] Total loaded: 19 records.

[DatabaseLoader] === DB Verification ===
  Total records     : 19
  Flagged records   : 8
  Total volume (USD): $170,743.75
  Unique countries  : 12

+==================================================+
|        ETL PIPELINE - DATA QUALITY REPORT       |
+==================================================+
  Run timestamp : 2026-02-19 18:42:58

--- PIPELINE SUMMARY ----------------------------------------
  Records read     : 23
  Records cleaned  : 20
  Records rejected : 3
  Records loaded   : 19
  Records flagged  : 8
  Pass rate        : 82.6%

--- TRANSACTION BREAKDOWN -----------------------------------
  By Transaction Type:
    ACH                  : 4
    CASH                 : 5
    DEBIT                : 1
    WIRE                 : 9
  Top 5 Countries:
    US                   : 8
    RU                   : 1
    AU                   : 1
    IN                   : 1
    JP                   : 1

--- FINANCIAL SUMMARY (USD) ---------------------------------
  Total volume : $     170,743.75
  Average txn  : $       8,986.51
  Largest txn  : $      75,000.00

--- REJECTION REASONS ---------------------------------------
  - Invalid email 'not-a-valid-email' for txn: TXN007
  - Missing customer ID: TXN008
  - Non-positive amount for txn: TXN009

=====================================================
Pipeline completed in 0.72 seconds.
```

## Project Structure

```
etl-pipeline/
├── src/
│   ├── main/java/com/etl/
│   │   ├── Main.java                    # Pipeline orchestrator
│   │   ├── model/
│   │   │   ├── Transaction.java         # Data model
│   │   │   └── ETLResult.java           # Pipeline stats
│   │   ├── reader/
│   │   │   └── CSVReader.java           # CSV extraction with error handling
│   │   ├── cleaner/
│   │   │   └── DataCleaner.java         # Validation & normalization
│   │   ├── transformer/
│   │   │   └── DataTransformer.java     # Business rules & AML flagging
│   │   ├── loader/
│   │   │   └── DatabaseLoader.java      # SQLite batch loading
│   │   └── reporter/
│   │       └── QualityReporter.java     # Data quality reporting
│   └── test/java/com/etl/
│       ├── cleaner/DataCleanerTest.java
│       └── transformer/DataTransformerTest.java
├── data/
│   └── transactions.csv                 # Sample data (with intentional dirty records)
└── pom.xml
```

## Tech Stack

- **Java 17** — Text blocks, modern language features
- **SQLite** via `sqlite-jdbc` — Embedded database, no setup required
- **Maven** — Build, dependency management, fat JAR packaging
- **JUnit 5** — Unit tests for core business logic

## Running It

**Prerequisites:** Java 17+, Maven 3.x

```bash
# Clone and build
git clone https://github.com/Amrit-B/customer-transaction-etl
cd etl-pipeline
mvn clean package -q

# Run with sample data
java -jar target/etl-pipeline-1.0.0-jar-with-dependencies.jar data/transactions.csv data/transactions.db

# Run with your own data
java -jar target/etl-pipeline-1.0.0-jar-with-dependencies.jar /path/to/input.csv /path/to/output.db

# Run tests
mvn test
```

## CSV Format

```
transaction_id,customer_id,full_name,phone,email,amount,currency,date,type,country
TXN001,CUST101,john smith,5551234567,john@example.com,500.00,USD,2024-01-15,WIRE,US
```

Supported date formats: `yyyy-MM-dd`, `MM/dd/yyyy`, `dd-MM-yyyy`  
Supported currencies: USD, EUR, GBP, INR, CAD, AUD, JPY, MXN (auto-converted to USD)

## Intentional Dirty Data in Sample

The included `transactions.csv` contains deliberate data quality issues to demonstrate the pipeline's robustness:
- Missing customer IDs
- Invalid email addresses
- Negative amounts
- Non-US phone formats
- Duplicate transaction IDs
- Multiple currency types
- Mixed date formats
- AML-triggerable transactions (high-risk countries, large cash, structuring)