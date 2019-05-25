package com.qiniu.statements;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class CsvReporter implements DataReporter {

    private String csvFilePath;
    private FileOutputStream fileOutputStream;
    private OutputStreamWriter osw;
    private String[] headers;
    private CSVFormat csvFormat;
    private CSVPrinter csvPrinter;

    public CsvReporter(String csvFilePath) throws IOException {
        this.csvFilePath = csvFilePath;
        this.fileOutputStream = new FileOutputStream(csvFilePath);
        this.osw = new OutputStreamWriter(fileOutputStream);
    }

    @Override
    public void setHeaders(String[] headers) {
        this.headers = headers;
    }

    @Override
    public void newFile(String filePath, String[] headers) throws IOException {
        this.csvFilePath = filePath;
        this.fileOutputStream = new FileOutputStream(filePath);
        this.osw = new OutputStreamWriter(fileOutputStream);
        this.headers = headers;
        this.csvFormat = CSVFormat.DEFAULT.withHeader(headers);
        this.csvPrinter = new CSVPrinter(osw, csvFormat);
    }

    @Override
    public String getReportFile() {
        return csvFilePath;
    }

    @Override
    public void insertData(Iterable<?> values) throws IOException {
        if (headers == null || headers.length == 0) throw new IOException("please set headers.");
        if (csvFormat == null) csvFormat = CSVFormat.DEFAULT.withHeader(headers);
        if (csvPrinter == null) csvPrinter = new CSVPrinter(osw, csvFormat);
        csvPrinter.printRecord(values);
    }

    @Override
    public void close() throws IOException {
        if (csvPrinter != null) csvPrinter.close();
        if (fileOutputStream != null) fileOutputStream.close();
        if (osw != null) osw.close();
    }
}
