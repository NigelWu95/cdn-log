package com.qiniu;

import com.qiniu.common.Config;
import com.qiniu.miaopai.LogAnalyse;
import com.qiniu.miaopai.Statistics;
import com.qiniu.statements.CsvReporter;
import com.qiniu.util.LogFileUtils;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.DatetimeUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
        List<String> dateTimeHours = DatetimeUtils.getDateTimeHours(startLocalDateTime, endLocalDateTime);
        List<String> logUrls = dateTimeHours.stream().map(dateTimeHour -> urlPattern.replace(replaced, dateTimeHour))
                .collect(Collectors.toList());
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
            List<Statistics> statisticsList = LogAnalyse.getAllStatistics(urlPattern, replaced, startLocalDateTime, endLocalDateTime);
            String[] headers = new String[]{"时间点", "总请求数", "卡顿率", "⾸帧加载时⻓长", "错误率"};
            csvReporter.setHeaders(headers);
            for (Statistics statistic : statisticsList) {
                List<String> values = new ArrayList<String>(){{
                    add(String.valueOf(statistic.getPointTime().toString()));
                    add(String.valueOf(statistic.getReqCount()));
                    add(String.valueOf(statistic.getCartonRate()));
                    add(String.valueOf(statistic.getValidLoadDurationAvg()));
                    add(String.valueOf(statistic.getErrorRate()));
                }};
                csvReporter.insertData(values);
            }
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
