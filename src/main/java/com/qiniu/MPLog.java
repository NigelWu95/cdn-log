package com.qiniu;

import com.alibaba.fastjson.JSON;

import java.util.Objects;

public class MPLog {

    private String uDid;
    private String vName;
    private String time;
    private String url;
    private String ip;
    private String serverIp;
    private int httpCode;
    private String error;
    private int videoViewLoadDuration;
    private int bufTimes;
    private int bufTimes2;
    private int bufAllTime;
    private String country;
    private String province;
    private String city;
    private String isp;

    public String getUDid() {
        return uDid;
    }

    public void setUDid(String uDid) {
        this.uDid = uDid;
    }

    public String getVName() {
        return vName;
    }

    public void setVName(String vName) {
        this.vName = vName;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getServerIp() {
        return serverIp;
    }

    public void setServerIp(String serverIp) {
        this.serverIp = serverIp;
    }

    public int getHttpCode() {
        return httpCode;
    }

    public void setHttpCode(int httpCode) {
        this.httpCode = httpCode;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public int getVideoViewLoadDuration() {
        return videoViewLoadDuration;
    }

    public void setVideoViewLoadDuration(int videoViewLoadDuration) {
        this.videoViewLoadDuration = videoViewLoadDuration;
    }

    public int getBufTimes() {
        return bufTimes;
    }

    public void setBufTimes(int bufTimes) {
        this.bufTimes = bufTimes;
    }

    public int getBufTimes2() {
        return bufTimes2;
    }

    public void setBufTimes2(int bufTimes2) {
        this.bufTimes2 = bufTimes2;
    }

    public int getBufAllTime() {
        return bufAllTime;
    }

    public void setBufAllTime(int bufAllTime) {
        this.bufAllTime = bufAllTime;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getIsp() {
        return isp;
    }

    public void setIsp(String isp) {
        this.isp = isp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(uDid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MPLog mpLog = (MPLog) o;
        return Objects.equals(uDid, mpLog.uDid);
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
