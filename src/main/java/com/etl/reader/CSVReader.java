package com.etl.reader;

import com.etl.model.Transaction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads raw transaction data from a CSV file.
 * Handles malformed rows gracefully and logs parse errors.
 */
public class CSVReader {

    private static final DateTimeFormatter[] DATE_FORMATS = {
        DateTimeFormatter.ofPattern("yyyy-MM-dd"),
        DateTimeFormatter.ofPattern("MM/dd/yyyy"),
        DateTimeFormatter.ofPattern("dd-MM-yyyy")
    };

    private int skippedRows = 0;

    public List<Transaction> read(String filePath) throws IOException {
        List<Transaction> transactions = new ArrayList<>();
        skippedRows = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String headerLine = br.readLine(); // skip header
            if (headerLine == null) {
                throw new IOException("CSV file is empty: " + filePath);
            }

            String line;
            int lineNumber = 1;

            while ((line = br.readLine()) != null) {
                lineNumber++;
                if (line.trim().isEmpty()) continue;

                try {
                    Transaction t = parseLine(line);
                    if (t != null) {
                        transactions.add(t);
                    }
                } catch (Exception e) {
                    System.err.printf("[CSVReader] Skipping malformed row %d: %s | Error: %s%n",
                            lineNumber, line, e.getMessage());
                    skippedRows++;
                }
            }
        }

        System.out.printf("[CSVReader] Read %d records. Skipped %d malformed rows.%n",
                transactions.size(), skippedRows);
        return transactions;
    }

    private Transaction parseLine(String line) {
        // Handle quoted fields containing commas
        String[] fields = splitCSV(line);

        if (fields.length < 10) {
            throw new IllegalArgumentException("Insufficient fields: expected 10, got " + fields.length);
        }

        Transaction t = new Transaction();
        t.setTransactionId(fields[0].trim());
        t.setCustomerId(fields[1].trim());
        t.setFullName(fields[2].trim());
        t.setPhone(fields[3].trim());
        t.setEmail(fields[4].trim());

        // Parse amount - may throw NumberFormatException
        String amountStr = fields[5].trim().replace("$", "").replace(",", "");
        t.setAmount(Double.parseDouble(amountStr));

        t.setCurrency(fields[6].trim().toUpperCase());
        t.setTransactionDate(parseDate(fields[7].trim()));
        t.setTransactionType(fields[8].trim());
        t.setCountry(fields[9].trim());
        t.setFlaggedForReview(false);
        t.setCleansingNotes("");

        return t;
    }

    private LocalDate parseDate(String dateStr) {
        for (DateTimeFormatter fmt : DATE_FORMATS) {
            try {
                return LocalDate.parse(dateStr, fmt);
            } catch (DateTimeParseException ignored) {}
        }
        throw new IllegalArgumentException("Unparseable date: " + dateStr);
    }

    /**
     * Splits a CSV line respecting quoted fields.
     */
    private String[] splitCSV(String line) {
        List<String> tokens = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;

        for (char c : line.toCharArray()) {
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                tokens.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        tokens.add(sb.toString());
        return tokens.toArray(new String[0]);
    }

    public int getSkippedRows() { return skippedRows; }
}
