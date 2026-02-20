package com.etl.model;

import java.util.ArrayList;
import java.util.List;

public class ETLResult {
    private int totalRead;
    private int totalCleaned;
    private int totalRejected;
    private int totalLoaded;
    private int totalFlagged;
    private List<String> rejectedRows = new ArrayList<>();
    private List<String> warnings = new ArrayList<>();

    public int getTotalRead() { return totalRead; }
    public void setTotalRead(int totalRead) { this.totalRead = totalRead; }

    public int getTotalCleaned() { return totalCleaned; }
    public void setTotalCleaned(int totalCleaned) { this.totalCleaned = totalCleaned; }

    public int getTotalRejected() { return totalRejected; }
    public void setTotalRejected(int totalRejected) { this.totalRejected = totalRejected; }

    public int getTotalLoaded() { return totalLoaded; }
    public void setTotalLoaded(int totalLoaded) { this.totalLoaded = totalLoaded; }

    public int getTotalFlagged() { return totalFlagged; }
    public void setTotalFlagged(int totalFlagged) { this.totalFlagged = totalFlagged; }

    public List<String> getRejectedRows() { return rejectedRows; }
    public void addRejectedRow(String row) { this.rejectedRows.add(row); }

    public List<String> getWarnings() { return warnings; }
    public void addWarning(String warning) { this.warnings.add(warning); }
}
