package com.qiniu;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.time.*;
import java.util.List;
import java.util.stream.Collectors;

public class LogFetch {

    public static void main( String[] args ) throws Exception {

        Config config = Config.getInstance();
        // http://cdnlog.bbobo.com/fd8c30aad0/qncdnbb_YYYYMMDD_HH.json.gz
        String urlPattern = config.getValue("url-pattern");
        String replaced = config.getValue("replaced");
        String startTime = config.getValue("start-time");
        String endTime = config.getValue("end-time");
        LocalDateTime startLocalDateTime = DatetimeUtils.parse(startTime);
        LocalDateTime endLocalDateTime = DatetimeUtils.parse(endTime);
        List<String> dateTimeHours = DatetimeUtils.getDateTimeHours(startLocalDateTime, endLocalDateTime);
        List<String> logs = dateTimeHours.stream()
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
            for (String url : logs) {
                while (true) {
                    try {
                        System.out.println(url + "\t" + bucketManager
                                .fetch(url, bucket, url.substring(url.lastIndexOf("/") + 1)).key);
                        break;
                    } catch (QiniuException e) {
                        if (e.code() == 404) break;
                        else if (!e.response.needRetry()) throw e;
                    }
                }
            }
        } else {
            File file = new File(saveTo);
            if (file.exists() && file.isDirectory()) {
                String fileName;
                for (String url : logs) {
                    fileName = FilenameUtils.concat(saveTo, url.substring(url.lastIndexOf("/") + 1));
                    System.out.println(fileName);
                    try {
                        FileUtils.copyURLToFile(new URL(url), new File(fileName));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                file = new File("logs" + System.getProperty("file.separator") + saveTo);
                boolean flag = file.createNewFile();
                if (!flag) throw new IOException("create new file: " + saveTo + " failed.");
                FileWriter fileWriter = new FileWriter(file);
                fileWriter.write(String.join("\n", logs));
                fileWriter.close();
            }
        }
    }
}
