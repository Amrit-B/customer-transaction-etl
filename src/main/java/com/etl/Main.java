package com.etl;

import com.etl.cleaner.DataCleaner;
import com.etl.loader.DatabaseLoader;
import com.etl.model.ETLResult;
import com.etl.model.Transaction;
import com.etl.reader.CSVReader;
import com.etl.reporter.QualityReporter;
import com.etl.transformer.DataTransformer;

import java.util.List;

/**
 * Entry point for the Customer Transaction ETL Pipeline.
 *
 * Pipeline stages:
 *   1. Extract  — Read raw CSV data
 *   2. Clean    — Validate, reject bad records, normalize fields
 *   3. Transform — Apply business rules, flag suspicious transactions
 *   4. Load     — Write to SQLite database
 *   5. Report   — Print data quality summary
 *
 * Usage:
 *   java -jar etl-pipeline.jar <input_csv> <output_db>
 *
 * Example:
 *   java -jar etl-pipeline.jar data/transactions.csv data/transactions.db
 */
public class Main {

    private static final String DEFAULT_INPUT  = "data/transactions.csv";
    private static final String DEFAULT_OUTPUT = "data/transactions.db";

    public static void main(String[] args) {
        String inputFile  = args.length > 0 ? args[0] : DEFAULT_INPUT;
        String outputDb   = args.length > 1 ? args[1] : DEFAULT_OUTPUT;

        System.out.println("+----------------------------------------------+");
        System.out.println("|   Customer Transaction ETL Pipeline v1.0    |");
        System.out.println("+----------------------------------------------+");
        System.out.printf("  Input  : %s%n", inputFile);
        System.out.printf("  Output : %s%n%n", outputDb);

        ETLResult result = new ETLResult();
        long startTime = System.currentTimeMillis();

        try {
            // ── STAGE 1: EXTRACT ──────────────────────────────────
            System.out.println("[Stage 1/4] Extracting data from CSV...");
            CSVReader reader = new CSVReader();
            List<Transaction> raw = reader.read(inputFile);
            result.setTotalRead(raw.size());

            // ── STAGE 2: CLEAN ────────────────────────────────────
            System.out.println("\n[Stage 2/4] Cleaning and validating records...");
            DataCleaner cleaner = new DataCleaner();
            List<Transaction> cleaned = cleaner.clean(raw);
            result.setTotalCleaned(cleaned.size());
            result.setTotalRejected(cleaner.getRejectedCount());
            cleaner.getRejectedReasons().forEach(result::addRejectedRow);

            // ── STAGE 3: TRANSFORM ────────────────────────────────
            System.out.println("\n[Stage 3/4] Applying business transformations...");
            DataTransformer transformer = new DataTransformer();
            List<Transaction> transformed = transformer.transform(cleaned);
            result.setTotalFlagged(transformer.getFlaggedCount());

            // ── STAGE 4: LOAD ─────────────────────────────────────
            System.out.println("\n[Stage 4/4] Loading to database...");
            DatabaseLoader loader = new DatabaseLoader(outputDb);
            int loaded = loader.load(transformed);
            result.setTotalLoaded(loaded);
            loader.printLoadSummary();

            // ── REPORT ────────────────────────────────────────────
            QualityReporter reporter = new QualityReporter();
            reporter.printReport(result, transformed);

            long elapsed = System.currentTimeMillis() - startTime;
            System.out.printf("Pipeline completed in %.2f seconds.%n", elapsed / 1000.0);

        } catch (Exception e) {
            System.err.println("\n[FATAL] Pipeline failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}