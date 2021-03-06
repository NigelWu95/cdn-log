package com.qiniu.log;

import com.alibaba.fastjson.JSON;

import java.time.LocalDateTime;

public class Statistic {

    private String province;
    private LocalDateTime pointTime;
    private long reqCount;
    private long loadDurationSum;
    private long loadDurationCount;
    private float loadDurationAvg; // 首帧加载时长 (ms)
    private long UV;
    private long kdUV;
    private float cartonRate; // 卡顿率
    private long errorCount;
    private float errorRate; // 错误率

    public Statistic(LocalDateTime pointTime, long reqCount, long loadDurationSum, long loadDurationCount,
                     long UV, long kdUV, long errorCount) {
        this.pointTime = pointTime;
        this.reqCount = reqCount;
        this.loadDurationSum = loadDurationSum;
        this.loadDurationCount = loadDurationCount;
        this.loadDurationAvg = (float) loadDurationSum / loadDurationCount;
        this.UV = UV;
        this.kdUV = kdUV;
        this.cartonRate = (float) kdUV / UV;
        this.errorCount = errorCount;
        this.errorRate = (float) errorCount / reqCount;
    }

    public String getProvince() {
        return province;
    }

    public Statistic withProvince(String province) {
        this.province = province;
        return this;
    }

    public LocalDateTime getPointTime() {
        return pointTime;
    }

    public long getReqCount() {
        return reqCount;
    }

    public long getLoadDurationSum() {
        return loadDurationSum;
    }

    public long getLoadDurationCount() {
        return loadDurationCount;
    }

    public float getLoadDurationAvg() {
        return loadDurationAvg;
    }

    public long getUV() {
        return UV;
    }

    public long getKdUV() {
        return kdUV;
    }

    public float getCartonRate() {
        return cartonRate;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public float getErrorRate() {
        return errorRate;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
