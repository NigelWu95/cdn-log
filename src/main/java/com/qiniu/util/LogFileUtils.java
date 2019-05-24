package com.qiniu.util;

import com.alibaba.fastjson.JSON;
import com.qiniu.common.QiniuException;
import com.qiniu.miaopai.MPLog;
import com.qiniu.storage.BucketManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class LogFileUtils {

    public static String logPath = "logs";

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

    public static void saveLogs(List<String> logUrls) throws IOException {
        File file = new File(logPath);
        if (!file.exists()) {
            boolean flag = file.mkdirs();
            if (!flag) throw new IOException("create new directory: " + logPath + " failed.");
        }
        if (file.isDirectory()) {
            String filePath;
            for (String logUrl : logUrls) {
                filePath = FilenameUtils.concat(logPath, logUrl.substring(logUrl.lastIndexOf("/") + 1));
                System.out.println(filePath);
                try {
                    FileUtils.copyURLToFile(new URL(logUrl), new File(filePath));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            throw new IOException("log path must be directory.");
        }
    }

    public static void listLogs(List<String> logUrls, String listName) throws IOException {
        File file = new File(FilenameUtils.concat(logPath, listName));
        boolean flag;
        if (!file.exists()) {
            flag = file.createNewFile();
            if (!flag) throw new IOException("create new file: " + listName + " failed.");
        }
        FileWriter fileWriter = new FileWriter(file, true);
        fileWriter.write(String.join("\n", logUrls));
        fileWriter.close();
    }

    public static String readAllFrom(InputStream inputStream) throws IOException {
        String result;
        GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[1024];
        int len = gzipInputStream.read(buf, 0, 1024);
        try {
            while(len != -1) {
                baos.write(buf, 0, len);
                len = gzipInputStream.read(buf, 0, 1024);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            result = baos.toString();
            try {
                gzipInputStream.close();
                baos.close();
            } catch (IOException e) {
                gzipInputStream = null;
                baos = null;
            }
        }
        return result;
    }

    public static String readAllFrom(String file) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        String content = readAllFrom(inputStream);
        try {
            inputStream.close();
        } catch (IOException e) {
            inputStream = null;
        }
        return content;
    }

    public static List<MPLog> readLogsFrom(InputStream inputStream) throws IOException {
        GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
        InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream);
        BufferedReader reader = new BufferedReader(inputStreamReader);
        String line;
        List<MPLog> mpLogs = new ArrayList<>();
        try {
            while ((line = reader.readLine()) != null) {
                mpLogs.add(JSON.parseObject(line, MPLog.class));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close();
            inputStreamReader.close();
            gzipInputStream.close();
            inputStream.close();
        }
        return mpLogs;
    }

    public static List<MPLog> readLogsFrom(String file) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        List<MPLog> mpLogs = readLogsFrom(inputStream);
        try {
            inputStream.close();
        } catch (IOException e) {
            inputStream = null;
        }
        return mpLogs;
    }

    public static List<MPLog> readLogsFrom(URL url) throws IOException {
        InputStream inputStream = url.openStream();
        List<MPLog> mpLogs = readLogsFrom(inputStream);
        try {
            inputStream.close();
        } catch (IOException e) {
            inputStream = null;
        }
        return mpLogs;
    }
}
