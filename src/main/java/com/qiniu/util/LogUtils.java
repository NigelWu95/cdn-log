package com.qiniu.util;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.List;

public class LogUtils {

    public static String logDir = "logs";

    public static void fetchLogs(List<String> logUrls, BucketManager bucketManager, String bucket) throws QiniuException {
        for (String logUrl : logUrls) {
            while (true) {
                try {
                    System.out.println(logUrl + "\t" + bucketManager
                            .fetch(logUrl, bucket, logUrl.substring(logUrl.lastIndexOf("/") + 1)).key);
                    break;
                } catch (QiniuException e) {
                    e.printStackTrace();
                    if (e.code() == 404) break;
                    else if (!e.response.needRetry()) throw e;
                }
            }
        }
    }

    public static void saveLogs(List<String> logUrls, String savePath) throws IOException {
        File file = new File(savePath);
        if (file.exists() && file.isDirectory()) {
            String filePath;
            for (String logUrl : logUrls) {
                filePath = FilenameUtils.concat(savePath, logUrl.substring(logUrl.lastIndexOf("/") + 1));
                System.out.println(filePath);
                try {
                    FileUtils.copyURLToFile(new URL(logUrl), new File(filePath));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            file = new File(FilenameUtils.concat(logDir, savePath));
            boolean flag;
            if (!file.exists()) {
                flag = file.createNewFile();
                if (!flag) throw new IOException("create new file: " + savePath + " failed.");
            }
            FileWriter fileWriter = new FileWriter(file, true);
            fileWriter.write(String.join("\n", logUrls));
            fileWriter.close();
        }
    }
}
