package com.etl.transformer;

import com.etl.model.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataTransformerTest {

    private DataTransformer transformer;

    @BeforeEach
    void setUp() {
        transformer = new DataTransformer();
    }

    private Transaction makeTransaction(String id, double amount, String currency, String type, String country) {
        Transaction t = new Transaction();
        t.setTransactionId(id);
        t.setCustomerId("CUST001");
        t.setFullName("Test User");
        t.setPhone("(555) 000-0000");
        t.setEmail("test@example.com");
        t.setAmount(amount);
        t.setCurrency(currency);
        t.setTransactionDate(LocalDate.now());
        t.setTransactionType(type);
        t.setCountry(country);
        t.setCleansingNotes("");
        t.setFlaggedForReview(false);
        return t;
    }

    @Test
    void testLargeCashTransactionFlagged() {
        Transaction t = makeTransaction("TXN001", 12000.0, "USD", "CASH", "US");
        List<Transaction> result = transformer.transform(List.of(t));
        assertTrue(result.get(0).isFlaggedForReview());
        assertEquals(1, transformer.getFlaggedCount());
    }

    @Test
    void testHighRiskCountryFlagged() {
        Transaction t = makeTransaction("TXN002", 100.0, "USD", "WIRE", "KP");
        List<Transaction> result = transformer.transform(List.of(t));
        assertTrue(result.get(0).isFlaggedForReview());
    }

    @Test
    void testPotentialStructuringFlagged() {
        Transaction t = makeTransaction("TXN003", 9500.0, "USD", "CASH", "US");
        List<Transaction> result = transformer.transform(List.of(t));
        assertTrue(result.get(0).isFlaggedForReview());
        assertTrue(result.get(0).getCleansingNotes().contains("POTENTIAL_STRUCTURING"));
    }

    @Test
    void testLargeWireTransferFlagged() {
        Transaction t = makeTransaction("TXN004", 60000.0, "USD", "WIRE", "US");
        List<Transaction> result = transformer.transform(List.of(t));
        assertTrue(result.get(0).isFlaggedForReview());
        assertTrue(result.get(0).getCleansingNotes().contains("LARGE_WIRE_TRANSFER"));
    }

    @Test
    void testCurrencyConversionEUR() {
        Transaction t = makeTransaction("TXN005", 1000.0, "EUR", "WIRE", "DE");
        List<Transaction> result = transformer.transform(List.of(t));
        assertEquals("USD", result.get(0).getCurrency());
        assertEquals(1080.0, result.get(0).getAmount(), 1.0);
        assertEquals(1, transformer.getCurrencyConversions());
    }

    @Test
    void testDuplicateTransactionRemoved() {
        Transaction t1 = makeTransaction("TXN006", 500.0, "USD", "WIRE", "US");
        Transaction t2 = makeTransaction("TXN006", 500.0, "USD", "WIRE", "US");
        List<Transaction> result = transformer.transform(List.of(t1, t2));
        assertEquals(1, result.size());
        assertEquals(1, transformer.getDuplicatesRemoved());
    }

    @Test
    void testNormalTransactionNotFlagged() {
        Transaction t = makeTransaction("TXN007", 200.0, "USD", "ACH", "US");
        List<Transaction> result = transformer.transform(List.of(t));
        assertFalse(result.get(0).isFlaggedForReview());
        assertEquals(0, transformer.getFlaggedCount());
    }
}
