package com.qiniu;

import com.qiniu.common.Config;
import com.qiniu.miaopai.LogAnalyse;
import com.qiniu.statements.CsvReporter;
import com.qiniu.util.LogUtils;
import com.qiniu.miaopai.MPLog;
import com.qiniu.miaopai.Statistics;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import com.qiniu.util.DatetimeUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class MainApp {

    public static void main(String[] args) throws IOException {

        Config config = Config.getInstance();
        String startTime = config.getValue("start-time");
        String endTime = config.getValue("end-time");
        LocalDateTime startLocalDateTime = DatetimeUtils.parse(startTime);
        LocalDateTime endLocalDateTime = DatetimeUtils.parse(endTime);
        LocalDateTime localDateTime = startLocalDateTime;
        String urlPattern = config.getValue("url-pattern");
        String replaced = config.getValue("replaced");
        String url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(localDateTime));
        List<String> dateTimeHours = DatetimeUtils.getDateTimeHours(startLocalDateTime, endLocalDateTime);
        List<String> logUrls = dateTimeHours.stream()
                .map(dateTimeHour -> urlPattern.replace(replaced, dateTimeHour))
                .collect(Collectors.toList());
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
            LogUtils.fetchLogs(logUrls, bucketManager, bucket);
        } else {
            LogUtils.saveLogs(logUrls, saveTo);
        }

        String fileName = LogUtils.logDir + url.substring(url.lastIndexOf("/") + 1);
        List<MPLog> logs = LogAnalyse.readToLogs(fileName);
        String[] headers = new String[]{"时间点", "总请求数", "卡顿率", "⾸帧加载时⻓长", "错误率"};
        CsvReporter csvReporter = new CsvReporter(LogUtils.logDir + startTime + "-" + endTime + ".csv", headers);
        while (localDateTime.compareTo(endLocalDateTime) <= 0) {
            LocalDateTime nextDateTime = localDateTime.plusHours(1);
            url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(nextDateTime));
            fileName = LogUtils.logDir + url.substring(url.lastIndexOf("/") + 1);
            List<MPLog> nextPhraseLogs;
            try {
                nextPhraseLogs = LogAnalyse.readToLogs(fileName);
            } catch (IOException e) {
                e.printStackTrace();
                nextPhraseLogs = new ArrayList<>();
            }
            logs.addAll(nextPhraseLogs);
            LocalDateTime finalLocalDateTime = localDateTime;
            List<Statistics> statisticsList = LogAnalyse.getStatistics(logs).stream()
                    .filter(statistics -> statistics.getPointTime().compareTo(nextDateTime) <= 0 &&
                            statistics.getPointTime().compareTo(finalLocalDateTime) > 0)
                    .sorted(Comparator.comparing(Statistics::getPointTime))
                    .collect(Collectors.toList());
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
            System.out.println(fileName + " finished");
            logs = nextPhraseLogs;
            localDateTime = nextDateTime;
        }
        try {
            csvReporter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
