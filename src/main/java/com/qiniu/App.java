package com.qiniu;

import com.qiniu.common.Config;
import com.qiniu.miaopai.LogAnalyse;
import com.qiniu.miaopai.MPLog;
import com.qiniu.miaopai.Statistics;
import com.qiniu.util.DatetimeUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class App {

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
        String fileName = "logs/" + url.substring(url.lastIndexOf("/") + 1);
        List<MPLog> logs = LogAnalyse.readToLogs(fileName);

        String[] headers = new String[]{"时间点", "总请求数", "卡顿率", "⾸帧加载时⻓长", "错误率"};
//        String resultCsv = "logs/" + startTime + "-" + endTime + ".csv";
        FileOutputStream fileOutputStream = new FileOutputStream("logs/" + startTime + "-" + endTime + ".csv");
        OutputStreamWriter osw = new OutputStreamWriter(fileOutputStream);
        CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader(headers);
        CSVPrinter csvPrinter = new CSVPrinter(osw, csvFormat);
        while (localDateTime.compareTo(endLocalDateTime) <= 0) {
            LocalDateTime nextDateTime = localDateTime.plusHours(1);
            url = urlPattern.replace(replaced, DatetimeUtils.getDateTimeHour(nextDateTime));
            fileName = "logs/" + url.substring(url.lastIndexOf("/") + 1);
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
                csvPrinter.printRecord(values);
            }
            System.out.println(fileName + " finished");
            logs = nextPhraseLogs;
            localDateTime = nextDateTime;
        }
        try {
            csvPrinter.close();
            fileOutputStream.close();
            osw.close();
        } catch (IOException e) {
            e.printStackTrace();
            csvPrinter = null;
            fileOutputStream = null;
            osw = null;
        }
    }
}
