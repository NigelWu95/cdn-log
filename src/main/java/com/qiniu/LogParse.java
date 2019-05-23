package com.qiniu;

import com.alibaba.fastjson.JSON;

import java.io.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class LogParse {

    private static final int BUFFER_SIZE = 1024;

    public static void main(String[] args) throws Exception {
        LogParse logParse = new LogParse();
        String file = "logs/qncdnbb_20190521_14.json.gz";
//        System.out.println(logParse.readAll(file));
        List<MPLog> logs = logParse.readAllTo(file);
//        System.out.println(logs);
//        for (MPLog log : logs) {
//            System.out.println(log.getError() + "\t" + log.getHttpCode());
//        }
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
            Statistics statistics = new Statistics(localDateTime, reqCount, videoViewLoadDurationSum, validReqCount,
                    UV, kdUV, errorCount);
            System.out.println(statistics);
            logSet.clear();
            kdLogSet.clear();
            errorLogs.clear();
        }
    }

//    "logs/qncdnbb_20190521_14.json.gz";
    public String readAll(String file) throws IOException {
        String result;
        GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(file));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[BUFFER_SIZE];
        int len = gzipInputStream.read(buf, 0, BUFFER_SIZE);
        try {
            while(len != -1) {
                baos.write(buf, 0, len);
//                baos.reset();
                len = gzipInputStream.read(buf, 0, BUFFER_SIZE);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            result = baos.toString();
            try {
                gzipInputStream.close();
                baos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public List<MPLog> readAllTo(String file) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(file);
        GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
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
            fileInputStream.close();
        }
        return mpLogs;
    }
}
