package com.etl.cleaner;

import com.etl.model.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Cleans and validates raw transaction records.
 * Rejects records that cannot be salvaged.
 * Fixes records where possible (phone normalization, name casing, etc.)
 */
public class DataCleaner {

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");

    private static final Pattern PHONE_DIGITS = Pattern.compile("[^0-9]");

    private int cleanedCount = 0;
    private int rejectedCount = 0;
    private final List<String> rejectedReasons = new ArrayList<>();

    public List<Transaction> clean(List<Transaction> raw) {
        List<Transaction> cleaned = new ArrayList<>();
        cleanedCount = 0;
        rejectedCount = 0;
        rejectedReasons.clear();

        for (Transaction t : raw) {
            if (isRejectable(t)) {
                rejectedCount++;
                continue;
            }
            applyFixes(t);
            cleaned.add(t);
            cleanedCount++;
        }

        System.out.printf("[DataCleaner] Cleaned: %d | Rejected: %d%n", cleanedCount, rejectedCount);
        return cleaned;
    }

    /**
     * Hard rejection rules — these records cannot be fixed.
     */
    private boolean isRejectable(Transaction t) {
        if (isBlank(t.getTransactionId())) {
            rejectedReasons.add("Missing transaction ID: " + t);
            return true;
        }
        if (isBlank(t.getCustomerId())) {
            rejectedReasons.add("Missing customer ID: " + t.getTransactionId());
            return true;
        }
        if (t.getAmount() <= 0) {
            rejectedReasons.add("Non-positive amount for txn: " + t.getTransactionId());
            return true;
        }
        if (t.getTransactionDate() == null) {
            rejectedReasons.add("Null date for txn: " + t.getTransactionId());
            return true;
        }
        if (!isValidEmail(t.getEmail())) {
            rejectedReasons.add("Invalid email '" + t.getEmail() + "' for txn: " + t.getTransactionId());
            return true;
        }
        return false;
    }

    /**
     * Soft fixes — normalize and standardize valid records.
     */
    private void applyFixes(Transaction t) {
        StringBuilder notes = new StringBuilder();

        // Normalize name to Title Case
        String originalName = t.getFullName();
        String titledName = toTitleCase(originalName);
        if (!titledName.equals(originalName)) {
            t.setFullName(titledName);
            notes.append("Name normalized; ");
        }

        // Normalize phone: strip non-digits, ensure 10-digit US format
        String rawPhone = PHONE_DIGITS.matcher(t.getPhone()).replaceAll("");
        if (rawPhone.startsWith("1") && rawPhone.length() == 11) {
            rawPhone = rawPhone.substring(1);
        }
        if (rawPhone.length() == 10) {
            String formattedPhone = String.format("(%s) %s-%s",
                    rawPhone.substring(0, 3),
                    rawPhone.substring(3, 6),
                    rawPhone.substring(6));
            if (!formattedPhone.equals(t.getPhone())) {
                t.setPhone(formattedPhone);
                notes.append("Phone normalized; ");
            }
        } else {
            t.setPhone("UNKNOWN");
            notes.append("Phone unparseable, set to UNKNOWN; ");
        }

        // Normalize currency
        t.setCurrency(t.getCurrency().toUpperCase().trim());

        // Normalize transaction type
        t.setTransactionType(t.getTransactionType().toUpperCase().trim());

        // Normalize country
        t.setCountry(t.getCountry().toUpperCase().trim());

        // Normalize email to lowercase
        t.setEmail(t.getEmail().toLowerCase().trim());

        if (notes.length() > 0) {
            t.setCleansingNotes(notes.toString().trim());
        }
    }

    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private boolean isValidEmail(String email) {
        if (isBlank(email)) return false;
        return EMAIL_PATTERN.matcher(email.trim()).matches();
    }

    private String toTitleCase(String input) {
        if (isBlank(input)) return input;
        String[] words = input.trim().toLowerCase().split("\\s+");
        StringBuilder sb = new StringBuilder();
        for (String word : words) {
            if (!word.isEmpty()) {
                sb.append(Character.toUpperCase(word.charAt(0)));
                sb.append(word.substring(1));
                sb.append(" ");
            }
        }
        return sb.toString().trim();
    }

    public int getCleanedCount() { return cleanedCount; }
    public int getRejectedCount() { return rejectedCount; }
    public List<String> getRejectedReasons() { return rejectedReasons; }
}
