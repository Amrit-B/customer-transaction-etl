package com.etl.transformer;

import com.etl.model.Transaction;

import java.util.*;

/**
 * Applies business transformation rules to cleaned transactions.
 *
 * Transformations include:
 * - Currency normalization to USD
 * - AML/fraud flagging (large cash transactions, high-risk countries)
 * - Transaction type standardization
 * - Duplicate detection
 */
public class DataTransformer {

    // Simplified exchange rates to USD
    private static final Map<String, Double> EXCHANGE_RATES = new HashMap<>();
    static {
        EXCHANGE_RATES.put("USD", 1.0);
        EXCHANGE_RATES.put("EUR", 1.08);
        EXCHANGE_RATES.put("GBP", 1.27);
        EXCHANGE_RATES.put("INR", 0.012);
        EXCHANGE_RATES.put("CAD", 0.74);
        EXCHANGE_RATES.put("AUD", 0.65);
        EXCHANGE_RATES.put("JPY", 0.0067);
        EXCHANGE_RATES.put("MXN", 0.058);
    }

    // FATF high-risk / monitored jurisdictions (simplified)
    private static final Set<String> HIGH_RISK_COUNTRIES = new HashSet<>(Arrays.asList(
            "MM", "IQ", "IR", "KP", "SY", "YE", "AF", "LY", "SO"
    ));

    // AML threshold: transactions above this amount in USD require review
    private static final double AML_THRESHOLD = 10_000.0;

    private int flaggedCount = 0;
    private int currencyConversions = 0;
    private final Set<String> seenIds = new HashSet<>();
    private int duplicatesRemoved = 0;

    public List<Transaction> transform(List<Transaction> cleaned) {
        List<Transaction> transformed = new ArrayList<>();
        flaggedCount = 0;
        currencyConversions = 0;
        seenIds.clear();
        duplicatesRemoved = 0;

        for (Transaction t : cleaned) {

            // Deduplicate on transaction ID
            if (seenIds.contains(t.getTransactionId())) {
                System.err.printf("[Transformer] Duplicate transaction ID found and removed: %s%n",
                        t.getTransactionId());
                duplicatesRemoved++;
                continue;
            }
            seenIds.add(t.getTransactionId());

            // Convert currency to USD
            convertToUSD(t);

            // Apply AML / fraud flagging rules
            applyFlaggingRules(t);

            transformed.add(t);
        }

        System.out.printf("[Transformer] Transformed: %d | Flagged: %d | Duplicates removed: %d | Currency conversions: %d%n",
                transformed.size(), flaggedCount, duplicatesRemoved, currencyConversions);

        return transformed;
    }

    private void convertToUSD(Transaction t) {
        String currency = t.getCurrency();
        if (!"USD".equals(currency)) {
            double rate = EXCHANGE_RATES.getOrDefault(currency, 1.0);
            double usdAmount = t.getAmount() * rate;
            t.setAmount(Math.round(usdAmount * 100.0) / 100.0);
            t.setCurrency("USD");
            t.setCleansingNotes(t.getCleansingNotes() +
                    String.format(" Converted from %s (rate=%.4f);", currency, rate));
            currencyConversions++;
        }
    }

    private void applyFlaggingRules(Transaction t) {
        List<String> flags = new ArrayList<>();

        // Rule 1: Large cash transactions (CTR threshold)
        if ("CASH".equals(t.getTransactionType()) && t.getAmount() >= AML_THRESHOLD) {
            flags.add("LARGE_CASH_TRANSACTION");
        }

        // Rule 2: High-risk country
        if (HIGH_RISK_COUNTRIES.contains(t.getCountry())) {
            flags.add("HIGH_RISK_COUNTRY");
        }

        // Rule 3: Structuring detection — just below AML threshold
        if (t.getAmount() >= 9_000.0 && t.getAmount() < AML_THRESHOLD) {
            flags.add("POTENTIAL_STRUCTURING");
        }

        // Rule 4: Unusually large transfer
        if (t.getAmount() > 50_000.0) {
            flags.add("LARGE_WIRE_TRANSFER");
        }

        if (!flags.isEmpty()) {
            t.setFlaggedForReview(true);
            t.setCleansingNotes(t.getCleansingNotes() + " FLAGS: " + String.join(", ", flags) + ";");
            flaggedCount++;
        }
    }

    public int getFlaggedCount() { return flaggedCount; }
    public int getDuplicatesRemoved() { return duplicatesRemoved; }
    public int getCurrencyConversions() { return currencyConversions; }
}
