package com.qiniu.statements;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

public class CsvReporter {

    private String csvFilePath;
    private FileOutputStream fileOutputStream;
    private OutputStreamWriter osw;
    private CSVFormat csvFormat;
    private CSVPrinter csvPrinter;

    public CsvReporter(String csvFilePath, String[] dataHeaders) throws IOException {
        this.csvFilePath = csvFilePath;
        this.fileOutputStream = new FileOutputStream(csvFilePath);
        this.osw = new OutputStreamWriter(fileOutputStream);
        this.csvFormat = CSVFormat.DEFAULT.withHeader(dataHeaders);
        this.csvPrinter = new CSVPrinter(osw, csvFormat);
    }

    public void newCsvFile(String csvFilePath, String[] dataHeaders) throws IOException {
        this.csvFilePath = csvFilePath;
        this.fileOutputStream = new FileOutputStream(csvFilePath);
        this.osw = new OutputStreamWriter(fileOutputStream);
        this.csvFormat = CSVFormat.DEFAULT.withHeader(dataHeaders);
        this.csvPrinter = new CSVPrinter(osw, csvFormat);
    }

    public void newFileOutputStream(FileOutputStream fileOutputStream) throws IOException {
        this.fileOutputStream = fileOutputStream;
        this.osw = new OutputStreamWriter(fileOutputStream);
        this.csvPrinter = new CSVPrinter(osw, csvFormat);
    }

    public void setOutputStreamWriter(OutputStreamWriter osw) throws IOException {
        this.osw = osw;
        this.csvPrinter = new CSVPrinter(osw, csvFormat);
    }

    public void setCsvFormat(CSVFormat csvFormat) throws IOException {
        this.csvFormat = csvFormat;
        this.csvPrinter = new CSVPrinter(osw, csvFormat);
    }

    public void setCsvPrinter(CSVPrinter csvPrinter) {
        this.csvPrinter = csvPrinter;
    }

    public String getCsvFilePath() {
        return csvFilePath;
    }

    public void insertData(List<String> values) throws IOException {
        csvPrinter.printRecord(values);
    }

    public void close() throws IOException {
        if (csvPrinter != null) csvPrinter.close();
        if (fileOutputStream != null) fileOutputStream.close();
        if (osw != null) osw.close();
    }
}
