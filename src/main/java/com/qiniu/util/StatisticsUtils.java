package com.qiniu.util;

import com.qiniu.miaopai.Statistics;
import com.qiniu.statements.DataReporter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

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
}
