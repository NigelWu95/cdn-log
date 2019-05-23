package com.qiniu;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;

public class App {

    public static void main(String[] args) throws IOException {

        Config config = Config.getInstance();
        String startTime = config.getValue("start-time");
        String endTime = config.getValue("end-time");
        String[] startDatetime = startTime.split("_");
        int startYear = Integer.valueOf(startDatetime[0].substring(0, 4));
        int startMonth = Integer.valueOf(startDatetime[0].substring(4, 6));
        int startDay = Integer.valueOf(startDatetime[0].substring(6, 8));
        int startHour = Integer.valueOf(startDatetime[1]);
        String[] endDatetime = endTime.split("_");
        int endYear = Integer.valueOf(endDatetime[0].substring(0, 4));
        int endMonth = Integer.valueOf(endDatetime[0].substring(4, 6));
        int endDay = Integer.valueOf(endDatetime[0].substring(6, 8));
        int endHour = Integer.valueOf(endDatetime[1]);
        LocalDateTime startLocalDateTime = LocalDateTime.of(
                LocalDate.of(startYear, startMonth, startDay), LocalTime.of(startHour, 0));
        LocalDateTime endLocalDateTime = LocalDateTime.of(
                LocalDate.of(endYear, endMonth, endDay), LocalTime.of(endHour, 0));
        LocalDateTime localDateTime = startLocalDateTime;
        String urlPattern = config.getValue("url-pattern");
        String replaced = config.getValue("replaced");
        String url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(localDateTime));
        String fileName = "logs/" + url.substring(url.lastIndexOf("/") + 1);
        List<MPLog> logs = LogAnalyse.readToLogs(fileName);
        url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(localDateTime.plusHours(1)));
        fileName = "logs/" + url.substring(url.lastIndexOf("/") + 1);
        logs.addAll(LogAnalyse.readToLogs(fileName));
        List<Statistics> statisticsList = LogAnalyse.getStatistics(logs).stream()
                .filter(statistics -> statistics.getPointTime().compareTo(localDateTime.plusSeconds(3600)) <= 0)
                .collect(Collectors.toList());
        for (Statistics statistic : statisticsList) System.out.println(statistic);
    }
}
