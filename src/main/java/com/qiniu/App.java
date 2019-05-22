package com.qiniu;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;

import java.time.*;

public class App {

    public static void main( String[] args ) throws Exception {

        Config config = Config.getInstance();
        String accessKey = config.getValue("ak");
        String secretKey = config.getValue("sk");
        String bucket = config.getValue("bucket");
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration();
        configuration.connectTimeout = 120;
        configuration.readTimeout = 60;
        BucketManager bucketManager = new BucketManager(auth, configuration);
        // http://cdnlog.bbobo.com/fd8c30aad0/qncdnbb_YYYYMMDD_HH.json.gz
        String urlPattern = config.getValue("url");
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
        StringBuilder datetimeString = new StringBuilder();
        String url;
        String key;
        while (localDateTime.compareTo(endLocalDateTime) <= 0) {
            datetimeString.append(localDateTime.getYear());
            if (localDateTime.getMonthValue() < 10) datetimeString.append(0);
            datetimeString.append(localDateTime.getMonthValue());
            if (localDateTime.getDayOfMonth() < 10) datetimeString.append(0);
            datetimeString.append(localDateTime.getDayOfMonth());
            datetimeString.append("_");
            if (localDateTime.getHour() < 10) datetimeString.append(0);
            datetimeString.append(localDateTime.getHour());
            url = urlPattern.replace("YYYYMMDD_HH", datetimeString.toString());
            System.out.println(url);
            key = url.substring(url.lastIndexOf("/") + 1);
            while (true) {
                try {
                    System.out.println(bucketManager.fetch(url, bucket, key).key);
                    break;
                } catch (QiniuException e) {
                    if (e.code() == 404) break;
                    else if (!e.response.needRetry()) throw e;
                }
            }
            datetimeString.delete(0, datetimeString.length());
            localDateTime = localDateTime.plusHours(1);
        }
    }
}
