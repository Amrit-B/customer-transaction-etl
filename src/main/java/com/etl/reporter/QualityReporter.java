package com.etl.reporter;

import com.etl.model.ETLResult;
import com.etl.model.Transaction;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Generates a human-readable data quality report after the ETL run.
 * Prints to console and writes to a report file.
 */
public class QualityReporter {

    public void printReport(ETLResult result, List<Transaction> loaded) {
        StringBuilder sb = new StringBuilder();
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        sb.append("\n+==================================================+\n");
        sb.append("|        ETL PIPELINE - DATA QUALITY REPORT       |\n");
        sb.append("+==================================================+\n");
        sb.append(String.format("  Run timestamp : %s%n", timestamp));
        sb.append("\n--- PIPELINE SUMMARY ----------------------------------------\n");
        sb.append(String.format("  Records read     : %d%n", result.getTotalRead()));
        sb.append(String.format("  Records cleaned  : %d%n", result.getTotalCleaned()));
        sb.append(String.format("  Records rejected : %d%n", result.getTotalRejected()));
        sb.append(String.format("  Records loaded   : %d%n", result.getTotalLoaded()));
        sb.append(String.format("  Records flagged  : %d%n", result.getTotalFlagged()));

        double passRate = result.getTotalRead() > 0
                ? (result.getTotalLoaded() * 100.0 / result.getTotalRead()) : 0;
        sb.append(String.format("  Pass rate        : %.1f%%%n", passRate));

        if (!loaded.isEmpty()) {
            sb.append("\n--- TRANSACTION BREAKDOWN -----------------------------------\n");

            // By type
            Map<String, Long> byType = loaded.stream()
                    .collect(Collectors.groupingBy(Transaction::getTransactionType, TreeMap::new, Collectors.counting()));
            sb.append("  By Transaction Type:\n");
            byType.forEach((k, v) -> sb.append(String.format("    %-20s : %d%n", k, v)));

            // By country (top 5)
            sb.append("  Top 5 Countries:\n");
            loaded.stream()
                    .collect(Collectors.groupingBy(Transaction::getCountry, Collectors.counting()))
                    .entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(5)
                    .forEach(e -> sb.append(String.format("    %-20s : %d%n", e.getKey(), e.getValue())));

            // Volume stats
            double totalVolume = loaded.stream().mapToDouble(Transaction::getAmount).sum();
            double avgAmount = loaded.stream().mapToDouble(Transaction::getAmount).average().orElse(0);
            double maxAmount = loaded.stream().mapToDouble(Transaction::getAmount).max().orElse(0);

            sb.append("\n--- FINANCIAL SUMMARY (USD) ---------------------------------\n");
            sb.append(String.format("  Total volume : $%,15.2f%n", totalVolume));
            sb.append(String.format("  Average txn  : $%,15.2f%n", avgAmount));
            sb.append(String.format("  Largest txn  : $%,15.2f%n", maxAmount));
        }

        if (!result.getRejectedRows().isEmpty()) {
            sb.append("\n--- REJECTION REASONS ---------------------------------------\n");
            result.getRejectedRows().stream().limit(10)
                    .forEach(r -> sb.append("  - ").append(r).append("\n"));
            if (result.getRejectedRows().size() > 10) {
                sb.append(String.format("  ... and %d more.%n", result.getRejectedRows().size() - 10));
            }
        }

        sb.append("\n=====================================================\n");

        System.out.println(sb);
        writeToFile(sb.toString(), "etl_report_" +
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".txt");
    }

    private void writeToFile(String content, String filename) {
        try (FileWriter fw = new FileWriter(filename)) {
            fw.write(content);
            System.out.printf("[QualityReporter] Report saved to: %s%n", filename);
        } catch (IOException e) {
            System.err.println("[QualityReporter] Could not write report: " + e.getMessage());
        }
    }
}