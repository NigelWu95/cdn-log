package com.qiniu.statements;

import java.io.IOException;

public interface DataReporter {

    void setHeaders(String[] headers);

    void newFile(String csvFilePath, String[] headers) throws IOException;

    String getReportFile();

    void insertData(Iterable<?> values) throws IOException;

    void close() throws IOException;
}
