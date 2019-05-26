package com.qiniu;

import com.qiniu.common.Config;
import com.qiniu.miaopai.LogAnalyse;
import com.qiniu.miaopai.Statistic;
import com.qiniu.statements.CsvReporter;
import com.qiniu.statements.DataReporter;
import com.qiniu.util.LogFileUtils;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.DatetimeUtils;
import com.qiniu.util.StatisticsUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
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
        List<String> dateTimeHours = DatetimeUtils.getDateTimeHours(startLocalDateTime, endLocalDateTime);
        List<String> logUrls = dateTimeHours.stream().map(dateTimeHour -> urlPattern.replace(replaced, dateTimeHour))
                .collect(Collectors.toList());
        DataReporter csvReporter = null;
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
            csvReporter = new CsvReporter("statistics/" + startTime + "-" + endTime + ".csv");
//            List<Statistic> statistics = LogAnalyse.statistics(urlPattern, replaced, startLocalDateTime, endLocalDateTime);
//            StatisticsUtils.exportTo(statistics, csvReporter);
//            StatisticsUtils.exportWeightedDayAvgTo(statistics, csvReporter);

//            List<Statistic> statistics = LogAnalyse.statisticsWithProvince(
//                    urlPattern, replaced, startLocalDateTime, endLocalDateTime);
//            StatisticsUtils.exportWithProvinceTo(statistics, startTime, endTime);

            Set<String> excludeProvinces = new HashSet<String>(){{
                add("西藏");
                add("贵州");
                add("四川");
                add("甘肃");
                add("北京");
//                add("广东");
//                add("云南");
//                add("广西");
//                add("浙江");
//                add("福建");
//                add("河南");
            }};
            List<Statistic> statistics = LogAnalyse.statisticsExcludeProvinces(
                    urlPattern, replaced, startLocalDateTime, endLocalDateTime, excludeProvinces);
            StatisticsUtils.exportTo(statistics, csvReporter);
        } else {
            LogFileUtils.listLogs(logUrls, goal);
        }
        try {
            if (csvReporter != null) csvReporter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
