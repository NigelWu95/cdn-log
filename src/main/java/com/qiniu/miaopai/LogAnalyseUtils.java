package com.qiniu.miaopai;

import com.qiniu.util.DatetimeUtils;
import com.qiniu.util.LogFileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class LogAnalyseUtils {

    public static List<Statistic> statistics(String file) throws IOException {
        return statistics(LogFileUtils.readLogsFrom(file));
    }

    public static List<Statistic> statistics(URL url) throws IOException {
        return statistics(LogFileUtils.readLogsFrom(url));
    }

    public static List<Statistic> statistics(InputStream inputStream) throws IOException {
        return statistics(LogFileUtils.readLogsFrom(inputStream));
    }

    public static List<Statistic> statistics(List<MPLog> logs) {
        List<Statistic> statistics = new ArrayList<>();
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
                if (code != 0 && code != -456 && code != -459 && duration >= 1 && duration <= 60000) {
//                    if (mpLog.getHttpCode() != 403)
                        errorLogs.add(mpLog);
                }
                return mpLog.getVideoViewLoadDuration();
            }).filter(duration -> duration >= 1 && duration <= 60000).collect(Collectors.toList());

//        logSet.addAll(logs);
            long reqCount = groupedLogs.size();
            long UV = logSet.size();
//                logs.parallelStream().distinct().count();
            long kdUV = kdLogSet.size();
//                logs.parallelStream().filter(mpLog -> mpLog.getBufTimes() > 0).distinct().count();
            long loadDurationCount = validDurations.size();
            long loadDurationSum = validDurations.parallelStream().reduce(Long::sum).orElse(0L);
//                logs.parallelStream().collect(Collectors.summarizingLong(MPLog::getVideoViewLoadDuration)).getSum();
            long errorCount = errorLogs.size();
            statistics.add(new Statistic(localDateTime, reqCount, loadDurationSum, loadDurationCount, UV, kdUV, errorCount));
            logSet.clear();
            kdLogSet.clear();
            errorLogs.clear();
        }
        return statistics;
    }

    public static List<Statistic> statisticsWithProvince(List<MPLog> logs) {
        List<Statistic> statistics = new ArrayList<>();
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
                long reqCount = provinceGroupedLogs.size();
                long errorCount = errorLogs.size();
                long UV = logSet.size();
                long kdUV = kdLogSet.size();
                long loadDurationCount = validDurations.size();
                long loadDurationSum = validDurations.parallelStream().reduce(Long::sum).orElse(0L);
                statistics.add(new Statistic(localDateTime, reqCount, loadDurationSum, loadDurationCount, UV, kdUV, errorCount)
                        .withProvince(province));
                logSet.clear();
                kdLogSet.clear();
                errorLogs.clear();
            }
        }
        return statistics;
    }

    public static List<Statistic> statistics(String urlPattern, String replaced, LocalDateTime startLocalDateTime,
                                             LocalDateTime endLocalDateTime) throws IOException {
        LocalDateTime localDateTime = startLocalDateTime;
        String url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(localDateTime));
        String fileName = FilenameUtils.concat(LogFileUtils.logPath, url.substring(url.lastIndexOf("/") + 1));
        List<MPLog> logs = LogFileUtils.readLogsFrom(fileName);
        List<Statistic> statistics = new ArrayList<>();
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
            statistics.addAll(LogAnalyseUtils.statistics(logs).stream()
                    .filter(statistic -> statistic.getPointTime().compareTo(nextDateTime) <= 0 &&
                            statistic.getPointTime().compareTo(finalLocalDateTime) > 0)
                    .collect(Collectors.toList()));
            System.out.println(fileName + " finished");
            logs = nextPhraseLogs;
            localDateTime = nextDateTime;
        }
        return statistics;
    }

    public static List<Statistic> statisticsWithProvince(String urlPattern, String replaced, LocalDateTime startLocalDateTime,
                                                               LocalDateTime endLocalDateTime) throws IOException {
        LocalDateTime localDateTime = startLocalDateTime;
        String url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(localDateTime));
        String fileName = FilenameUtils.concat(LogFileUtils.logPath, url.substring(url.lastIndexOf("/") + 1));
        List<MPLog> logs = LogFileUtils.readLogsFrom(fileName);
        List<Statistic> statistics = new ArrayList<>();
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
            statistics.addAll(LogAnalyseUtils.statisticsWithProvince(logs).stream()
                    .filter(statistic -> statistic.getPointTime().compareTo(nextDateTime) <= 0 &&
                            statistic.getPointTime().compareTo(finalLocalDateTime) > 0)
                    .collect(Collectors.toList()));
            System.out.println(fileName + " finished");
            logs = nextPhraseLogs;
            localDateTime = nextDateTime;
        }
        return statistics;
    }

    public static List<Statistic> statisticsExcludeProvinces(String urlPattern, String replaced, LocalDateTime startLocalDateTime,
                                                                   LocalDateTime endLocalDateTime, Set<String> provinces)
            throws IOException {
        LocalDateTime localDateTime = startLocalDateTime;
        String url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(localDateTime));
        String fileName = FilenameUtils.concat(LogFileUtils.logPath, url.substring(url.lastIndexOf("/") + 1));
        List<MPLog> logs = LogFileUtils.readLogsFrom(fileName).stream()
                .filter(mpLog -> !provinces.contains(mpLog.getProvince()))
                .collect(Collectors.toList());
        List<Statistic> statistics = new ArrayList<>();
        while (localDateTime.compareTo(endLocalDateTime) <= 0) {
            LocalDateTime nextDateTime = localDateTime.plusHours(1);
            url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(nextDateTime));
            fileName = FilenameUtils.concat(LogFileUtils.logPath, url.substring(url.lastIndexOf("/") + 1));
            List<MPLog> nextPhraseLogs;
            try {
                nextPhraseLogs = LogFileUtils.readLogsFrom(fileName).stream()
                        .filter(mpLog -> !provinces.contains(mpLog.getProvince()))
                        .collect(Collectors.toList());
            } catch (IOException e) {
                e.printStackTrace();
                nextPhraseLogs = new ArrayList<>();
            }
            logs.addAll(nextPhraseLogs);
            LocalDateTime finalLocalDateTime = localDateTime;
            statistics.addAll(LogAnalyseUtils.statistics(logs).stream()
                    .filter(statistic -> statistic.getPointTime().compareTo(nextDateTime) <= 0 &&
                            statistic.getPointTime().compareTo(finalLocalDateTime) > 0)
                    .collect(Collectors.toList()));
            System.out.println(fileName + " finished");
            logs = nextPhraseLogs;
            localDateTime = nextDateTime;
        }
        return statistics;
    }
}
