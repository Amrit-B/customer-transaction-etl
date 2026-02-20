package com.etl.loader;

import com.etl.model.Transaction;

import java.sql.*;
import java.util.List;

/**
 * Loads transformed transaction records into a SQLite database.
 * Creates schema if it doesn't exist.
 * Uses batch inserts for performance.
 */
public class DatabaseLoader {

    private final String dbUrl;
    private static final int BATCH_SIZE = 100;

    public DatabaseLoader(String dbPath) {
        this.dbUrl = "jdbc:sqlite:" + dbPath;
    }

    public int load(List<Transaction> transactions) throws SQLException {
        initSchema();
        return batchInsert(transactions);
    }

    private void initSchema() throws SQLException {
        String createTable = """
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id   TEXT PRIMARY KEY,
                    customer_id      TEXT NOT NULL,
                    full_name        TEXT,
                    phone            TEXT,
                    email            TEXT,
                    amount_usd       REAL NOT NULL,
                    transaction_date TEXT NOT NULL,
                    transaction_type TEXT,
                    country          TEXT,
                    flagged          INTEGER DEFAULT 0,
                    cleansing_notes  TEXT,
                    loaded_at        TEXT DEFAULT (datetime('now'))
                )
                """;

        String createFlaggedView = """
                CREATE VIEW IF NOT EXISTS flagged_transactions AS
                SELECT * FROM transactions WHERE flagged = 1
                """;

        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute(createTable);
            stmt.execute(createFlaggedView);
            System.out.println("[DatabaseLoader] Schema initialized.");
        }
    }

    private int batchInsert(List<Transaction> transactions) throws SQLException {
        String insertSQL = """
                INSERT OR REPLACE INTO transactions
                (transaction_id, customer_id, full_name, phone, email,
                 amount_usd, transaction_date, transaction_type, country,
                 flagged, cleansing_notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        int loaded = 0;

        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {

            conn.setAutoCommit(false);

            for (Transaction t : transactions) {
                pstmt.setString(1, t.getTransactionId());
                pstmt.setString(2, t.getCustomerId());
                pstmt.setString(3, t.getFullName());
                pstmt.setString(4, t.getPhone());
                pstmt.setString(5, t.getEmail());
                pstmt.setDouble(6, t.getAmount());
                pstmt.setString(7, t.getTransactionDate().toString());
                pstmt.setString(8, t.getTransactionType());
                pstmt.setString(9, t.getCountry());
                pstmt.setInt(10, t.isFlaggedForReview() ? 1 : 0);
                pstmt.setString(11, t.getCleansingNotes());
                pstmt.addBatch();
                loaded++;

                if (loaded % BATCH_SIZE == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    System.out.printf("[DatabaseLoader] Committed %d records...%n", loaded);
                }
            }

            // Commit remaining
            pstmt.executeBatch();
            conn.commit();
        }

        System.out.printf("[DatabaseLoader] Total loaded: %d records.%n", loaded);
        return loaded;
    }

    /**
     * Quick verification query after load.
     */
    public void printLoadSummary() throws SQLException {
        String query = """
                SELECT
                    COUNT(*) AS total,
                    SUM(flagged) AS flagged,
                    ROUND(SUM(amount_usd), 2) AS total_volume_usd,
                    COUNT(DISTINCT country) AS unique_countries
                FROM transactions
                """;

        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            if (rs.next()) {
                System.out.println("\n[DatabaseLoader] === DB Verification ===");
                System.out.printf("  Total records     : %d%n", rs.getInt("total"));
                System.out.printf("  Flagged records   : %d%n", rs.getInt("flagged"));
                System.out.printf("  Total volume (USD): $%,.2f%n", rs.getDouble("total_volume_usd"));
                System.out.printf("  Unique countries  : %d%n", rs.getInt("unique_countries"));
            }
        }
    }
}
