package com.qiniu;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LogFetch {

    public static void main( String[] args ) throws Exception {

        Config config = Config.getInstance();
        // http://cdnlog.bbobo.com/fd8c30aad0/qncdnbb_YYYYMMDD_HH.json.gz
        String urlPattern = config.getValue("url-pattern");
        String replaced = config.getValue("replaced");
        String startTime = config.getValue("start-time");
        String endTime = config.getValue("end-time");
        LogFetch logFetch = new LogFetch();
        List<String> logs = logFetch.getLogUrls(urlPattern, replaced, startTime, endTime);
        String saveTo = config.getValue("save-to");
        if (saveTo.equals("qiniu")) {
            String accessKey = config.getValue("ak");
            String secretKey = config.getValue("sk");
            String bucket = config.getValue("bucket");
            Auth auth = Auth.create(accessKey, secretKey);
            Configuration configuration = new Configuration();
            configuration.connectTimeout = 120;
            configuration.readTimeout = 60;
            BucketManager bucketManager = new BucketManager(auth, configuration);
            for (String url : logs) {
                while (true) {
                    try {
                        System.out.println(url + "\t" + bucketManager
                                .fetch(url, bucket, url.substring(url.lastIndexOf("/") + 1)).key);
                        break;
                    } catch (QiniuException e) {
                        if (e.code() == 404) break;
                        else if (!e.response.needRetry()) throw e;
                    }
                }
            }
        } else {
            File file = new File(saveTo);
            if (file.exists() && file.isDirectory()) {
                String fileName;
                for (String url : logs) {
                    fileName = FilenameUtils.concat(saveTo, url.substring(url.lastIndexOf("/") + 1));
                    System.out.println(fileName);
                    try {
                        FileUtils.copyURLToFile(new URL(url), new File(fileName));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                file = new File("logs" + System.getProperty("file.separator") + saveTo);
                boolean flag = file.createNewFile();
                if (!flag) throw new IOException("create new file: " + saveTo + " failed.");
                FileWriter fileWriter = new FileWriter(file);
                fileWriter.write(String.join("\n", logs));
                fileWriter.close();
            }
        }
    }

    public List<String> getLogUrls(String urlPattern, String replaced, String startTime, String endTime) {
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
        List<String> dateTimeHours = getDateTimeHours(startLocalDateTime, endLocalDateTime);
        return dateTimeHours.stream().map(dateTimeHour -> urlPattern.replace(replaced, dateTimeHour)).collect(Collectors.toList());
    }

    public List<String> getDateTimeHours(LocalDateTime startLocalDateTime, LocalDateTime endLocalDateTime) {
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
