package com.qiniu;

import com.qiniu.common.Config;
import com.qiniu.miaopai.LogAnalyse;
import com.qiniu.miaopai.Statistics;
import com.qiniu.statements.CsvReporter;
import com.qiniu.statements.DataReporter;
import com.qiniu.util.LogFileUtils;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.DatetimeUtils;
import com.qiniu.util.StatisticsUtils;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileOutputStream;
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
            List<Statistics> statisticsList = LogAnalyse.getAllStatistics(urlPattern, replaced, startLocalDateTime, endLocalDateTime);
            csvReporter = new CsvReporter("statistics/" + startTime + "-" + endTime + ".csv");
            StatisticsUtils.exportAllTo(statisticsList, csvReporter);
//            StatisticsUtils.exportWeightedDayAvgTo(statisticsList, csvReporter);

//            List<Statistics> statisticsList = LogAnalyse.getAllStatisticsWithProvince(
//                    urlPattern, replaced, startLocalDateTime, endLocalDateTime);
//            StatisticsUtils.exportAllWithProvinceTo(statisticsList, startTime, endTime);
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
