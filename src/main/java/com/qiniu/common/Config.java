package com.qiniu.common;

import java.io.*;
import java.util.Properties;

/**
 * ClassName: Config
 * Description: 读取账号配置信息
 */
public class Config {

    private static Config config;
    private Properties properties;

    public Config() throws IOException {
        properties = new Properties();
        InputStream inStream = this.getClass().getResourceAsStream("/.config.properties");
        properties.load(inStream);
    }

    public static synchronized Config getInstance() throws IOException {
        if (config == null) {
            config = new Config();
        }

        return config;
    }

    /**
     * 获取属性值，判断是否存在相应的 key，不存在或 value 为空则抛出异常
     * @param key 属性名
     * @return 属性值字符
     * @throws IOException 无法获取参数值或者参数值为空时抛出异常
     */
    public String getValue(String key) throws IOException {
        if ("".equals(properties.getProperty(key, ""))) {
            throw new IOException("not set \"" + key + "\" param.");
        } else {
            return properties.getProperty(key);
        }
    }
}
