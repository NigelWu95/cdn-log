package com.qiniu.miaopai;

import java.time.LocalDateTime;

public class ErrorStatistic {

    private LocalDateTime pointTime;
    private int errorCode;
    private int httpCode;
    private int errorCount;
    private int httpCount;
    private int totalCount;

    public ErrorStatistic(LocalDateTime pointTime, int errorCode, int httpCode, int errorCount, int httpCount, int totalCount) {
        this.pointTime = pointTime;
        this.errorCode = errorCode;
        this.httpCode = httpCode;
        this.errorCount = errorCount;
        this.httpCount = httpCount;
        this.totalCount = totalCount;
    }

    public LocalDateTime getPointTime() {
        return pointTime;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public int getHttpCode() {
        return httpCode;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public int getHttpCount() {
        return httpCount;
    }

    public int getTotalCount() {
        return totalCount;
    }
}
