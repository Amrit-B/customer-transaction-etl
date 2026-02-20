package com.etl.model;

import java.time.LocalDate;

public class Transaction {
    private String transactionId;
    private String customerId;
    private String fullName;
    private String phone;
    private String email;
    private double amount;
    private String currency;
    private LocalDate transactionDate;
    private String transactionType;
    private String country;
    private boolean flaggedForReview;
    private String cleansingNotes;

    public Transaction() {}

    // Getters and Setters
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getFullName() { return fullName; }
    public void setFullName(String fullName) { this.fullName = fullName; }

    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }

    public LocalDate getTransactionDate() { return transactionDate; }
    public void setTransactionDate(LocalDate transactionDate) { this.transactionDate = transactionDate; }

    public String getTransactionType() { return transactionType; }
    public void setTransactionType(String transactionType) { this.transactionType = transactionType; }

    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }

    public boolean isFlaggedForReview() { return flaggedForReview; }
    public void setFlaggedForReview(boolean flaggedForReview) { this.flaggedForReview = flaggedForReview; }

    public String getCleansingNotes() { return cleansingNotes; }
    public void setCleansingNotes(String cleansingNotes) { this.cleansingNotes = cleansingNotes; }

    @Override
    public String toString() {
        return String.format("Transaction{id='%s', customer='%s', amount=%.2f %s, date=%s, flagged=%b}",
                transactionId, customerId, amount, currency, transactionDate, flaggedForReview);
    }
}
