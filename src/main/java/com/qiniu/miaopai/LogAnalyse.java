package com.qiniu.miaopai;

import com.qiniu.util.DatetimeUtils;
import com.qiniu.util.LogFileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class LogAnalyse {

    public static List<Statistics> getStatistics(String file) throws IOException {
        return getStatistics(LogFileUtils.readLogsFrom(file));
    }

    public static List<Statistics> getStatistics(URL url) throws IOException {
        return getStatistics(LogFileUtils.readLogsFrom(url));
    }

    public static List<Statistics> getStatistics(InputStream inputStream) throws IOException {
        return getStatistics(LogFileUtils.readLogsFrom(inputStream));
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
            long loadDurationSum = validDurations.parallelStream().reduce(Long::sum).orElse(0L);
//                logs.parallelStream().collect(Collectors.summarizingLong(MPLog::getVideoViewLoadDuration)).getSum();
            long errorCount = errorLogs.size();
            statistics.add(new Statistics(localDateTime, reqCount, loadDurationSum, validReqCount, UV, kdUV, errorCount));
            logSet.clear();
            kdLogSet.clear();
            errorLogs.clear();
        }
        return statistics;
    }

    public static List<Statistics> getStatisticsWithProvince(List<MPLog> logs) {
        List<Statistics> statistics = new ArrayList<>();
        Set<MPLog> logSet = new HashSet<>();
        Set<MPLog> kdLogSet = new HashSet<>();
        List<MPLog> errorLogs = new ArrayList<>();
        Map<LocalDateTime, List<MPLog>> groupedMap = logs.parallelStream()
                .collect(Collectors.groupingBy(mpLog -> DatetimeUtils.groupedTimeBy5Min(mpLog.getTime())));
        List<MPLog> groupedLogs;
        List<MPLog> provinceGroupedLogs;
        for (LocalDateTime localDateTime : groupedMap.keySet()) {
            groupedLogs = groupedMap.get(localDateTime);
            Map<String, List<MPLog>> provinceGroupedMap = groupedLogs.parallelStream()
                    .collect(Collectors.groupingBy(MPLog::getProvince));
            for (String province : provinceGroupedMap.keySet()) {
                provinceGroupedLogs = provinceGroupedMap.get(province);
                List<Long> validDurations = provinceGroupedLogs.parallelStream().map(mpLog -> {
                    logSet.add(mpLog);
                    if (mpLog.getBufTimes() > 0) kdLogSet.add(mpLog);
                    int code = mpLog.getError();
                    long duration = mpLog.getVideoViewLoadDuration();
                    if (code != 0 && code != -456 && code != -459 && duration >= 1 && duration <= 60000) errorLogs.add(mpLog);
                    return mpLog.getVideoViewLoadDuration();
                }).filter(duration -> duration >= 1 && duration <= 60000).collect(Collectors.toList());
                long reqCount = groupedLogs.size();
                long UV = logSet.size();
                long kdUV = kdLogSet.size();
                long validReqCount = validDurations.size();
                long loadDurationSum = validDurations.parallelStream().reduce(Long::sum).orElse(0L);
                long errorCount = errorLogs.size();
                statistics.add(new Statistics(localDateTime, reqCount, loadDurationSum, validReqCount, UV, kdUV, errorCount)
                        .withProvince(province));
                logSet.clear();
                kdLogSet.clear();
                errorLogs.clear();
            }
        }
        return statistics;
    }

    public static List<Statistics> getAllStatistics(String urlPattern, String replaced, LocalDateTime startLocalDateTime,
                                                    LocalDateTime endLocalDateTime) throws IOException {
        LocalDateTime localDateTime = startLocalDateTime;
        String url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(localDateTime));
        String fileName = FilenameUtils.concat(LogFileUtils.logPath, url.substring(url.lastIndexOf("/") + 1));
        List<MPLog> logs = LogFileUtils.readLogsFrom(fileName);
        List<Statistics> statisticsList = new ArrayList<>();
        while (localDateTime.compareTo(endLocalDateTime) <= 0) {
            LocalDateTime nextDateTime = localDateTime.plusHours(1);
            url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(nextDateTime));
            fileName = FilenameUtils.concat(LogFileUtils.logPath, url.substring(url.lastIndexOf("/") + 1));
            List<MPLog> nextPhraseLogs;
            try {
                nextPhraseLogs = LogFileUtils.readLogsFrom(fileName);
            } catch (IOException e) {
                e.printStackTrace();
                nextPhraseLogs = new ArrayList<>();
            }
            logs.addAll(nextPhraseLogs);
            LocalDateTime finalLocalDateTime = localDateTime;
            statisticsList.addAll(LogAnalyse.getStatistics(logs).stream()
                    .filter(statistics -> statistics.getPointTime().compareTo(nextDateTime) <= 0 &&
                            statistics.getPointTime().compareTo(finalLocalDateTime) > 0)
                    .sorted(Comparator.comparing(Statistics::getPointTime))
                    .collect(Collectors.toList()));
            System.out.println(fileName + " finished");
            logs = nextPhraseLogs;
            localDateTime = nextDateTime;
        }
        return statisticsList;
    }

    public static List<Statistics> getAllStatisticsWithProvince(String urlPattern, String replaced, LocalDateTime startLocalDateTime,
                                                    LocalDateTime endLocalDateTime) throws IOException {
        LocalDateTime localDateTime = startLocalDateTime;
        String url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(localDateTime));
        String fileName = FilenameUtils.concat(LogFileUtils.logPath, url.substring(url.lastIndexOf("/") + 1));
        List<MPLog> logs = LogFileUtils.readLogsFrom(fileName);
        List<Statistics> statisticsList = new ArrayList<>();
        while (localDateTime.compareTo(endLocalDateTime) <= 0) {
            LocalDateTime nextDateTime = localDateTime.plusHours(1);
            url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(nextDateTime));
            fileName = FilenameUtils.concat(LogFileUtils.logPath, url.substring(url.lastIndexOf("/") + 1));
            List<MPLog> nextPhraseLogs;
            try {
                nextPhraseLogs = LogFileUtils.readLogsFrom(fileName);
            } catch (IOException e) {
                e.printStackTrace();
                nextPhraseLogs = new ArrayList<>();
            }
            logs.addAll(nextPhraseLogs);
            LocalDateTime finalLocalDateTime = localDateTime;
            statisticsList.addAll(LogAnalyse.getStatisticsWithProvince(logs).stream()
                    .filter(statistics -> statistics.getPointTime().compareTo(nextDateTime) <= 0 &&
                            statistics.getPointTime().compareTo(finalLocalDateTime) > 0)
                    .sorted(Comparator.comparing(Statistics::getPointTime))
                    .collect(Collectors.toList()));
            System.out.println(fileName + " finished");
            logs = nextPhraseLogs;
            localDateTime = nextDateTime;
        }
        return statisticsList;
    }
}
