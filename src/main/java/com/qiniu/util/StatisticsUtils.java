package com.qiniu.util;

import com.qiniu.miaopai.Statistics;
import com.qiniu.statements.DataReporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StatisticsUtils {

    public static void exportDataTo(List<Statistics> statisticsList, DataReporter dataReporter) throws IOException {
        String[] headers = new String[]{"时间点", "总请求数", "卡顿率", "⾸帧加载时⻓长", "错误率"};
        dataReporter.setHeaders(headers);
        for (Statistics statistic : statisticsList) {
            List<String> values = new ArrayList<String>(){{
                add(String.valueOf(statistic.getPointTime().toString()));
                add(String.valueOf(statistic.getReqCount()));
                add(String.valueOf(statistic.getCartonRate()));
                add(String.valueOf(statistic.getValidLoadDurationAvg()));
                add(String.valueOf(statistic.getErrorRate()));
            }};
            dataReporter.insertData(values);
        }
        try {
            dataReporter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
