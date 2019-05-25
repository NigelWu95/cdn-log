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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatisticsUtils {

    public static void exportAllTo(List<Statistics> statisticsList, DataReporter dataReporter) throws IOException {
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

    /**
     * // TODO
     */
    public static void exportWeightedDayAvgTo(List<Statistics> statisticsList, DataReporter dataReporter) throws IOException {
        long dayRepCount = 0;
        long dayValidRepCount = 0;
        long weightedLoadDurationSum = 0;
        long weightedCartonRateSum = 0;
        long weightedErrorRateSum = 0;
        LocalDateTime pointDatetime, dayDateTime, nextDay = null;
        String[] headers = new String[]{"时间", "总请求数", "有效请求数", "卡顿率", "⾸帧加载时⻓长", "错误率"};
        dataReporter.setHeaders(headers);
        for (Statistics statistic : statisticsList) {
            pointDatetime = statistic.getPointTime();
            if (nextDay != null && pointDatetime.isBefore(nextDay)) {
                dayRepCount += statistic.getReqCount();
                dayValidRepCount += statistic.getValidReqCount();
                weightedLoadDurationSum += statistic.getLoadDurationAvg() * statistic.getValidReqCount();
                weightedCartonRateSum += statistic.getCartonRate() * statistic.getReqCount();
                weightedErrorRateSum += statistic.getErrorRate() * statistic.getReqCount();
            } else {
                dayDateTime = LocalDateTime.of(pointDatetime.getYear(), pointDatetime.getMonth(),
                        pointDatetime.getDayOfMonth(), 0, 0);
                nextDay = dayDateTime.plusDays(1);
                String dayDateTimeString = dayDateTime.toString();
                long finalDayRepCount = dayRepCount;
                float loadDurationAvg = (float) weightedLoadDurationSum / dayValidRepCount;
                float cartonRate = (float) weightedCartonRateSum / dayRepCount;
                float errorRate = (float) weightedErrorRateSum / dayRepCount;
                long finalDayValidRepCount = dayValidRepCount;
                List<String> values = new ArrayList<String>(){{
                    add(dayDateTimeString);
                    add(String.valueOf(finalDayRepCount));
                    add(String.valueOf(finalDayValidRepCount));
                    add(String.valueOf(cartonRate));
                    add(String.valueOf(loadDurationAvg));
                    add(String.valueOf(errorRate));
                }};
                dataReporter.insertData(values);
            }
        }
    }

    public static void exportAllWithProvinceTo(List<Statistics> statisticsList, String startTime, String endTime)
            throws IOException {
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
