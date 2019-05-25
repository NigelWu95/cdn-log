package com.qiniu.statements;

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class PoiExcelReporter implements DataReporter {

    private String excelPath;
    private XSSFWorkbook workbook;
    private XSSFSheet spreadsheet;
    private String[] headers;
    private int headerLen;
    private FileOutputStream fileOutputStream;

    public PoiExcelReporter(String excelPath, String sheetName) throws FileNotFoundException {
        this.excelPath = excelPath;
        this.workbook = new XSSFWorkbook();
        this.spreadsheet = workbook.createSheet(sheetName);
        this.fileOutputStream = new FileOutputStream(new File(excelPath));
    }

    @Override
    public void setHeaders(String[] headers) {
        this.headers = headers;
        this.headerLen = headers.length;
    }

    @Override
    public void newFile(String filePath, String[] headers) throws IOException {
        this.excelPath = filePath;
        this.fileOutputStream = new FileOutputStream(new File(filePath));
        this.headers = headers;
    }

    @Override
    public String getReportFile() {
        return excelPath;
    }

    @Override
    public void insertData(Iterable<?> values) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
