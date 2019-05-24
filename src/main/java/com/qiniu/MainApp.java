package com.qiniu;

import com.qiniu.common.Config;
import com.qiniu.miaopai.LogAnalyse;
import com.qiniu.statements.CsvReporter;
import com.qiniu.util.LogFileUtils;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.DatetimeUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

public class MainApp {

    public static void main(String[] args) throws IOException {

        Config config = Config.getInstance();
        String startTime = config.getValue("start-time");
        String endTime = config.getValue("end-time");
        LocalDateTime startLocalDateTime = DatetimeUtils.parse(startTime);
        LocalDateTime endLocalDateTime = DatetimeUtils.parse(endTime);
        String urlPattern = config.getValue("url-pattern");
        String replaced = config.getValue("replaced");
        CsvReporter csvReporter = new CsvReporter("statistics/" + startTime + "-" + endTime + ".csv");
        LogAnalyse logAnalyse = new LogAnalyse(startLocalDateTime, endLocalDateTime, urlPattern, replaced);
        List<String> dateTimeHours = DatetimeUtils.getDateTimeHours(logAnalyse.getStartLocalDateTime(),
                logAnalyse.getEndLocalDateTime());
        List<String> logUrls = dateTimeHours.stream().map(logAnalyse::getRealUrl).collect(Collectors.toList());
        String goal = config.getValue("goal");
        if (goal.equals("fetch")) {
            String accessKey = config.getValue("ak");
            String secretKey = config.getValue("sk");
            String bucket = config.getValue("bucket");
            Auth auth = Auth.create(accessKey, secretKey);
            Configuration configuration = new Configuration();
            configuration.connectTimeout = 120;
            configuration.readTimeout = 60;
            BucketManager bucketManager = new BucketManager(auth, configuration);
            LogFileUtils.fetchLogs(logUrls, bucketManager, bucket);
        } else if (goal.equals("download")) {
            LogFileUtils.saveLogs(logUrls);
        } else if (goal.equals("analyse")) {
            logAnalyse.analyseLogs(csvReporter);
            try {
                csvReporter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            LogFileUtils.listLogs(logUrls, goal);
        }
    }
}
