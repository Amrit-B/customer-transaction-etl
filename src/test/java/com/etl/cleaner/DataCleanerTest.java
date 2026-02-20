package com.etl.cleaner;

import com.etl.model.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataCleanerTest {

    private DataCleaner cleaner;

    @BeforeEach
    void setUp() {
        cleaner = new DataCleaner();
    }

    private Transaction validTransaction() {
        Transaction t = new Transaction();
        t.setTransactionId("TXN001");
        t.setCustomerId("CUST001");
        t.setFullName("john smith");
        t.setPhone("5551234567");
        t.setEmail("john@example.com");
        t.setAmount(500.0);
        t.setCurrency("USD");
        t.setTransactionDate(LocalDate.now());
        t.setTransactionType("WIRE");
        t.setCountry("US");
        t.setCleansingNotes("");
        return t;
    }

    @Test
    void testValidTransactionPassesThrough() {
        List<Transaction> input = List.of(validTransaction());
        List<Transaction> result = cleaner.clean(input);
        assertEquals(1, result.size());
        assertEquals(0, cleaner.getRejectedCount());
    }

    @Test
    void testNameNormalizedToTitleCase() {
        Transaction t = validTransaction();
        t.setFullName("JOHN SMITH");
        List<Transaction> result = cleaner.clean(List.of(t));
        assertEquals("John Smith", result.get(0).getFullName());
    }

    @Test
    void testPhoneNormalizedToUSFormat() {
        Transaction t = validTransaction();
        t.setPhone("5551234567");
        List<Transaction> result = cleaner.clean(List.of(t));
        assertEquals("(555) 123-4567", result.get(0).getPhone());
    }

    @Test
    void testInternationalPhoneStripped() {
        Transaction t = validTransaction();
        t.setPhone("+15551234567");
        List<Transaction> result = cleaner.clean(List.of(t));
        assertEquals("(555) 123-4567", result.get(0).getPhone());
    }

    @Test
    void testInvalidEmailRejected() {
        Transaction t = validTransaction();
        t.setEmail("not-an-email");
        List<Transaction> result = cleaner.clean(List.of(t));
        assertEquals(0, result.size());
        assertEquals(1, cleaner.getRejectedCount());
    }

    @Test
    void testNegativeAmountRejected() {
        Transaction t = validTransaction();
        t.setAmount(-100.0);
        List<Transaction> result = cleaner.clean(List.of(t));
        assertEquals(0, result.size());
    }

    @Test
    void testMissingTransactionIdRejected() {
        Transaction t = validTransaction();
        t.setTransactionId("");
        List<Transaction> result = cleaner.clean(List.of(t));
        assertEquals(0, result.size());
    }

    @Test
    void testMissingCustomerIdRejected() {
        Transaction t = validTransaction();
        t.setCustomerId(null);
        List<Transaction> result = cleaner.clean(List.of(t));
        assertEquals(0, result.size());
    }

    @Test
    void testEmptyListReturnsEmpty() {
        List<Transaction> result = cleaner.clean(new ArrayList<>());
        assertTrue(result.isEmpty());
    }
}
