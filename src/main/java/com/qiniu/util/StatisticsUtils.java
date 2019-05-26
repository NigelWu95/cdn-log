package com.qiniu.util;

import com.qiniu.miaopai.Statistic;
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

    public static void exportTo(List<Statistic> statistics, DataReporter dataReporter) throws IOException {
        statistics.sort(Comparator.comparing(Statistic::getPointTime));
        String[] headers = new String[]{"时间点", "总请求数", "有效请求数", "卡顿率", "⾸帧加载时⻓长", "错误率"};
        dataReporter.setHeaders(headers);
        for (Statistic statistic : statistics) {
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

    public static void exportWeightedDayAvgTo(List<Statistic> statistics, DataReporter dataReporter) throws IOException {
        statistics.sort(Comparator.comparing(Statistic::getPointTime));
        long dayRepCount = 0;
        long dayValidRepCount = 0;
        long weightedLoadDurationSum = 0;
        long weightedCartonRateSum = 0;
        long weightedErrorRateSum = 0;
        LocalDateTime pointDatetime, nearNextDay = null;
        String[] headers = new String[]{"时间", "总请求数", "有效请求数", "卡顿率", "⾸帧加载时⻓长", "错误率"};
        dataReporter.setHeaders(headers);
        Statistic statistic;
        int size = statistics.size();
        boolean end = false;
        for (int i = 0; i < size; i++) {
            statistic = statistics.get(i);
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
            } else {
                end = true;
            }
            if (end || i == size - 1) {
                nearNextDay = LocalDateTime.of(pointDatetime.getYear(), pointDatetime.getMonth(),
                        pointDatetime.getDayOfMonth(), 0, 0).plusDays(1).minusNanos(1);
                String dayDateTimeString = (end ? nearNextDay.minusDays(1) : pointDatetime).toLocalDate().toString();
                float loadDurationAvg = (float) weightedLoadDurationSum / dayValidRepCount;
                float cartonRate = (float) weightedCartonRateSum / dayRepCount;
                float errorRate = (float) weightedErrorRateSum / dayRepCount;
                List<String> values = new ArrayList<String>(){{
                    add(dayDateTimeString + "-WeightedDayAvg");
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
                end = false;
            }
        }
    }

    public static void exportWithProvinceTo(List<Statistic> statistics, String startTime, String endTime) throws IOException {
        statistics.sort(Comparator.comparing(Statistic::getPointTime));
        List<LocalDateTime> localDateTimes = statistics.parallelStream().map(Statistic::getPointTime)
                .distinct().sorted().collect(Collectors.toList());
        List<String> provinces = statistics.parallelStream().map(Statistic::getProvince)
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
            for (Statistic statistic : statistics) {
                XSSFCell xssfCell = timeXSSFRowMap.get(statistic.getPointTime()).get(statistic.getProvince());
                switch (k) {
//                        case 0: xssfCell.setCellValue(statistic.getPointTime().toString());
                    case 0: xssfCell.setCellValue(statistic.getReqCount()); break;
                    case 1: xssfCell.setCellValue(statistic.getValidReqCount()); break;
                    case 2: xssfCell.setCellValue(statistic.getCartonRate()); break;
                    case 3: xssfCell.setCellValue(statistic.getLoadDurationAvg()); break;
                    case 4: xssfCell.setCellValue(statistic.getErrorRate()); break;
                }
            }
        }
        workbook.write(fileOutputStream);
        fileOutputStream.close();
    }
}
