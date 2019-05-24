package com.qiniu.util;

import java.time.*;
import java.util.ArrayList;
import java.util.List;

public class DatetimeUtils {

    private static ZoneId defaultZoneId = ZoneId.systemDefault();

    /**
     * 经过 5 分钟粒度对时间进行分组计算后处于的时间段点位
     * @param timestamp 精度为 s 的时间戳
     * @return
     */
    public static LocalDateTime groupedTimeBy5Min(long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond((timestamp / 300 + 1) * 300), defaultZoneId);
    }

    public static LocalDateTime parse(String datetimeText) {
        String[] startDatetime = datetimeText.split("_");
        int startYear = Integer.valueOf(startDatetime[0].substring(0, 4));
        int startMonth = Integer.valueOf(startDatetime[0].substring(4, 6));
        int startDay = Integer.valueOf(startDatetime[0].substring(6, 8));
        int startHour = Integer.valueOf(startDatetime[1]);
        return LocalDateTime.of(LocalDate.of(startYear, startMonth, startDay), LocalTime.of(startHour, 0));
    }

    public static String getDateTimeHour(LocalDateTime localDateTime) {
        StringBuilder datetimeString = new StringBuilder();
        datetimeString.append(localDateTime.getYear());
        if (localDateTime.getMonthValue() < 10) datetimeString.append(0);
        datetimeString.append(localDateTime.getMonthValue());
        if (localDateTime.getDayOfMonth() < 10) datetimeString.append(0);
        datetimeString.append(localDateTime.getDayOfMonth());
        datetimeString.append("_");
        if (localDateTime.getHour() < 10) datetimeString.append(0);
        datetimeString.append(localDateTime.getHour());
        return datetimeString.toString();
    }

    public static List<String> getDateTimeHours(LocalDateTime startLocalDateTime, LocalDateTime endLocalDateTime) {
        LocalDateTime localDateTime = startLocalDateTime;
        StringBuilder datetimeString = new StringBuilder();
        List<String> dateTimeHours = new ArrayList<>();
        while (localDateTime.compareTo(endLocalDateTime) <= 0) {
            datetimeString.append(localDateTime.getYear());
            if (localDateTime.getMonthValue() < 10) datetimeString.append(0);
            datetimeString.append(localDateTime.getMonthValue());
            if (localDateTime.getDayOfMonth() < 10) datetimeString.append(0);
            datetimeString.append(localDateTime.getDayOfMonth());
            datetimeString.append("_");
            if (localDateTime.getHour() < 10) datetimeString.append(0);
            datetimeString.append(localDateTime.getHour());
            dateTimeHours.add(datetimeString.toString());
            datetimeString.delete(0, datetimeString.length());
            localDateTime = localDateTime.plusHours(1);
        }
        return dateTimeHours;
    }
}
