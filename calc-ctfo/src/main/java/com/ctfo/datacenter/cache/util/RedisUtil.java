package com.ctfo.datacenter.cache.util;

public class RedisUtil {
    public static String getKey(String dbname, String tablename, String key) {
        return dbname + "-" + tablename + "-" + key;
    }

    public static String getConfigKey(String key) {
        return "cfg-sys-" + key;
    }

    public static String getConfigUserKey(String key) {
        return "cfg-usr-" + key;
    }

    public static String getConfigKey(String tablename, String key) {
        return "cfg-" + tablename + "-" + key;
    }

    public static String getConfigPatternKey(String tablename) {
        return "cfg-" + tablename + "-" + "*";
    }

    public static String getDataPatternKey(String dbname, String tablename) {
        return dbname + "-" + tablename + "-" + "*";
    }

    public static String getDataPatternKey(String dbname) {
        return dbname + "-" + "*";
    }
}
