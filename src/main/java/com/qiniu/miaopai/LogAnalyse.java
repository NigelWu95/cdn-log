package com.qiniu.miaopai;

import com.alibaba.fastjson.JSON;
import com.qiniu.util.DatetimeUtils;

import java.io.*;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class LogAnalyse {

    public static List<Statistics> getStatistics(String file) throws IOException {
        return getStatistics(readToLogs(file));
    }

    public static List<Statistics> getStatistics(URL url) throws IOException {
        return getStatistics(readToLogs(url));
    }

    public static List<Statistics> getStatistics(InputStream inputStream) throws IOException {
        return getStatistics(readToLogs(inputStream));
    }

    public static List<Statistics> getStatistics(List<MPLog> logs) {
        List<Statistics> statistics = new ArrayList<>();
        Set<MPLog> logSet = new HashSet<>();
        Set<MPLog> kdLogSet = new HashSet<>();
        List<MPLog> errorLogs = new ArrayList<>();
        Map<LocalDateTime, List<MPLog>> groupedMap = logs.parallelStream()
                .collect(Collectors.groupingBy(mpLog -> DatetimeUtils.groupedTimeBy5Min(mpLog.getTime())));
        List<MPLog> groupedLogs;
        for (LocalDateTime localDateTime : groupedMap.keySet()) {
            groupedLogs = groupedMap.get(localDateTime);
            List<Long> validDurations = groupedLogs.parallelStream().map(mpLog -> {
                logSet.add(mpLog);
                if (mpLog.getBufTimes() > 0) kdLogSet.add(mpLog);
                int code = mpLog.getError();
                long duration = mpLog.getVideoViewLoadDuration();
                if (code != 0 && code != -456 && code != -459 && duration >= 1 && duration <= 60000) errorLogs.add(mpLog);
                return mpLog.getVideoViewLoadDuration();
            }).filter(duration -> duration >= 1 && duration <= 60000).collect(Collectors.toList());

//        logSet.addAll(logs);
            long reqCount = groupedLogs.size();
            long UV = logSet.size();
//                logs.parallelStream().distinct().count();
            long kdUV = kdLogSet.size();
//                logs.parallelStream().filter(mpLog -> mpLog.getBufTimes() > 0).distinct().count();
            long validReqCount = validDurations.size();
            long videoViewLoadDurationSum = validDurations.parallelStream().reduce(Long::sum).orElse(0L);
//                logs.parallelStream().collect(Collectors.summarizingLong(MPLog::getVideoViewLoadDuration)).getSum();
            long errorCount = errorLogs.size();
            statistics.add(new Statistics(localDateTime, reqCount, videoViewLoadDurationSum, validReqCount, UV, kdUV, errorCount));
            logSet.clear();
            kdLogSet.clear();
            errorLogs.clear();
        }
        return statistics;
    }

    public static String readAll(InputStream inputStream) throws IOException {
        String result;
        GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[1024];
        int len = gzipInputStream.read(buf, 0, 1024);
        try {
            while(len != -1) {
                baos.write(buf, 0, len);
                len = gzipInputStream.read(buf, 0, 1024);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            result = baos.toString();
            try {
                gzipInputStream.close();
                baos.close();
            } catch (IOException e) {
                gzipInputStream = null;
                baos = null;
            }
        }
        return result;
    }

    public static String readAll(String file) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        String content = readAll(inputStream);
        try {
            inputStream.close();
        } catch (IOException e) {
            inputStream = null;
        }
        return content;
    }

    public static List<MPLog> readToLogs(InputStream inputStream) throws IOException {
        GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
        InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream);
        BufferedReader reader = new BufferedReader(inputStreamReader);
        String line;
        List<MPLog> mpLogs = new ArrayList<>();
        try {
            while ((line = reader.readLine()) != null) {
                mpLogs.add(JSON.parseObject(line, MPLog.class));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close();
            inputStreamReader.close();
            gzipInputStream.close();
            inputStream.close();
        }
        return mpLogs;
    }

    public static List<MPLog> readToLogs(String file) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        List<MPLog> mpLogs = readToLogs(inputStream);
        try {
            inputStream.close();
        } catch (IOException e) {
            inputStream = null;
        }
        return mpLogs;
    }

    public static List<MPLog> readToLogs(URL url) throws IOException {
        InputStream inputStream = url.openStream();
        List<MPLog> mpLogs = readToLogs(inputStream);
        try {
            inputStream.close();
        } catch (IOException e) {
            inputStream = null;
        }
        return mpLogs;
    }
}
