package com.qiniu;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

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
    }
}
