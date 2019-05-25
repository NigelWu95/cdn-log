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
//            List<Statistics> statisticsList = LogAnalyse.getAllStatistics(urlPattern, replaced, startLocalDateTime, endLocalDateTime);
//            csvReporter = new CsvReporter("statistics/" + startTime + "-" + endTime + ".csv");
//            StatisticsUtils.exportAllTo(statisticsList, csvReporter);
//            StatisticsUtils.exportWeightedDayAvgTo(statisticsList, csvReporter);

            List<Statistics> statisticsList = LogAnalyse.getAllStatisticsWithProvince(
                    urlPattern, replaced, startLocalDateTime, endLocalDateTime);
            List<LocalDateTime> localDateTimes = statisticsList.parallelStream().map(Statistics::getPointTime)
                    .distinct().sorted().collect(Collectors.toList());
            List<String> provinces = statisticsList.parallelStream().map(Statistics::getProvince)
                    .distinct().collect(Collectors.toList());
            XSSFWorkbook workbook = new XSSFWorkbook();
            String[] sheets = new String[]{"总请求数", "有效请求数", "卡顿率", "⾸帧加载时⻓长", "错误率"};
//            String[] sheets = new String[]{"时间点", "总请求数", "有效请求数", "卡顿率", "⾸帧加载时⻓长", "错误率"};
            FileOutputStream fileOutputStream = new FileOutputStream(
                    new File("statistics/province-" + startTime + "-" + endTime + ".xlsx"));
            for (int k = 0; k < sheets.length; k++) {
                XSSFSheet spreadsheet = workbook.createSheet(sheets[k]);
                Map<LocalDateTime, Map<String, XSSFCell>> timeXSSFRowMap = new HashMap<>();
                for (int i = -1; i < localDateTimes.size(); i++) {
                    XSSFRow row = spreadsheet.createRow(i + 1);
                    Map<String, XSSFCell> cellMap = new HashMap<>();
                    for (int j = -1; j < provinces.size(); j++) {
                        XSSFCell xssfCell = row.createCell(j + 1);
                        if (j == -1) {
                            if (i > -1) xssfCell.setCellValue(localDateTimes.get(i).toString());
                        } else {
                            if (i == -1) {
                                xssfCell.setCellValue(provinces.get(j));
                            } else {
                                cellMap.put(provinces.get(j), xssfCell);
                            }
                        }
                    }
                    if (i >= 0) timeXSSFRowMap.put(localDateTimes.get(i), cellMap);
                }
                for (Statistics statistics : statisticsList) {
                    XSSFCell xssfCell = timeXSSFRowMap.get(statistics.getPointTime()).get(statistics.getProvince());
                    switch (k) {
//                        case 0: xssfCell.setCellValue(statistics.getPointTime().toString());
                        case 0: xssfCell.setCellValue(statistics.getReqCount()); break;
                        case 1: xssfCell.setCellValue(statistics.getValidReqCount()); break;
                        case 2: xssfCell.setCellValue(statistics.getCartonRate()); break;
                        case 3: xssfCell.setCellValue(statistics.getLoadDurationAvg()); break;
                        case 4: xssfCell.setCellValue(statistics.getErrorRate()); break;
                    }
                }
            }
            workbook.write(fileOutputStream);
            fileOutputStream.close();
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
