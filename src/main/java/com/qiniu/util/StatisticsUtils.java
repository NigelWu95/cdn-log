package com.qiniu.util;

import com.qiniu.miaopai.Statistics;
import com.qiniu.statements.DataReporter;
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

public class StatisticsUtils {

    public static void exportAllTo(List<Statistics> statisticsList, DataReporter dataReporter) throws IOException {
        statisticsList.sort(Comparator.comparing(Statistics::getPointTime));
        String[] headers = new String[]{"时间点", "总请求数", "有效请求数", "卡顿率", "⾸帧加载时⻓长", "错误率"};
        dataReporter.setHeaders(headers);
        for (Statistics statistic : statisticsList) {
            List<String> values = new ArrayList<String>(){{
                add(String.valueOf(statistic.getPointTime().toString()));
                add(String.valueOf(statistic.getReqCount()));
                add(String.valueOf(statistic.getValidReqCount()));
                add(String.valueOf(statistic.getCartonRate()));
                add(String.valueOf(statistic.getLoadDurationAvg()));
                add(String.valueOf(statistic.getErrorRate()));
            }};
            dataReporter.insertData(values);
        }
    }

    public static void exportWeightedDayAvgTo(List<Statistics> statisticsList, DataReporter dataReporter) throws IOException {
        statisticsList.sort(Comparator.comparing(Statistics::getPointTime));
        long dayRepCount = 0;
        long dayValidRepCount = 0;
        long weightedLoadDurationSum = 0;
        long weightedCartonRateSum = 0;
        long weightedErrorRateSum = 0;
        LocalDateTime pointDatetime, nearNextDay = null;
        String[] headers = new String[]{"时间", "总请求数", "有效请求数", "卡顿率", "⾸帧加载时⻓长", "错误率"};
        dataReporter.setHeaders(headers);
        Statistics statistic;
        int size = statisticsList.size();
        for (int i = 0; i < size; i++) {
            statistic = statisticsList.get(i);
            pointDatetime = statistic.getPointTime();
            if (nearNextDay == null) {
                nearNextDay = LocalDateTime.of(pointDatetime.getYear(), pointDatetime.getMonth(),
                        pointDatetime.getDayOfMonth(), 0, 0).plusDays(1);
            }
            if (pointDatetime.isBefore(nearNextDay)) {
                dayRepCount += statistic.getReqCount();
                dayValidRepCount += statistic.getValidReqCount();
                weightedLoadDurationSum += statistic.getValidReqCount() * statistic.getLoadDurationAvg();
                weightedCartonRateSum += statistic.getReqCount() * statistic.getCartonRate();
                weightedErrorRateSum += statistic.getReqCount() * statistic.getErrorRate();
            }
            if (pointDatetime.isAfter(nearNextDay) || i == size - 1) {
                nearNextDay = LocalDateTime.of(pointDatetime.getYear(), pointDatetime.getMonth(),
                        pointDatetime.getDayOfMonth(), 0, 0).plusDays(1).minusNanos(1);
                String dayDateTimeString = nearNextDay.minusDays(1).toLocalDate().toString();
                float loadDurationAvg = (float) weightedLoadDurationSum / dayValidRepCount;
                float cartonRate = (float) weightedCartonRateSum / dayRepCount;
                float errorRate = (float) weightedErrorRateSum / dayRepCount;
                List<String> values = new ArrayList<String>(){{
                    add(dayDateTimeString);
//                    add(String.valueOf(finalDayRepCount));
//                    add(String.valueOf(finalDayValidRepCount));
                    add("0");
                    add("0");
                    add(String.valueOf(cartonRate));
                    add(String.valueOf(loadDurationAvg));
                    add(String.valueOf(errorRate));
                }};
                dataReporter.insertData(values);
                dayRepCount = statistic.getReqCount();
                dayValidRepCount = statistic.getValidReqCount();
                weightedLoadDurationSum = (long) (statistic.getValidReqCount() * statistic.getLoadDurationAvg());
                weightedCartonRateSum = (long) (statistic.getReqCount() * statistic.getCartonRate());
                weightedErrorRateSum = (long) (statistic.getReqCount() * statistic.getErrorRate());
            }
        }
    }

    public static void exportAllWithProvinceTo(List<Statistics> statisticsList, String startTime, String endTime) throws IOException {
        statisticsList.sort(Comparator.comparing(Statistics::getPointTime));
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
    }
}
