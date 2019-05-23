package com.qiniu;

import com.alibaba.fastjson.JSON;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class LogParse {

    private static final int BUFFER_SIZE = 1024;

    public static void main(String[] args) throws Exception {
        LogParse logParse = new LogParse();
        String file = "logs/qncdnbb_20190521_14.json.gz";
//        System.out.println(logParse.readAll(file));
        List<MPLog> logs = logParse.readAllTo(file);
//        System.out.println(logs);
        Set<MPLog> logSet = new HashSet<>(logs);
        System.out.println(logs.size());
        System.out.println(logSet.size());
        long UV = logs.parallelStream().distinct().count();
        System.out.println(UV);
        long kdUV = logs.parallelStream().filter(mpLog -> mpLog.getBufTimes() > 0).distinct().count();
        System.out.println(kdUV);
    }

//    "logs/qncdnbb_20190521_14.json.gz";
    public String readAll(String file) throws IOException {
        String result;
        GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(file));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[BUFFER_SIZE];
        int len = gzipInputStream.read(buf, 0, BUFFER_SIZE);
        try {
            while(len != -1) {
                baos.write(buf, 0, len);
//                baos.reset();
                len = gzipInputStream.read(buf, 0, BUFFER_SIZE);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            result = baos.toString();
            try {
                gzipInputStream.close();
                baos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public List<MPLog> readAllTo(String file) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(file);
        GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
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
            fileInputStream.close();
        }
        return mpLogs;
    }
}
