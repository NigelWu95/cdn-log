package com.qiniu;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

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
}
