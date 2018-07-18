package com.ctfo.datacenter.cache;

public class Constant {
    public static final String SYNC = "sync";
    public static final String MONITOR = "monitor";
    public static final String THRIFT = "thrift";
    public static final String CACHE = "cache";
    public static final String CONFIG_DB = "cfg";
    public static final String USER_TABLE = "usr";
    public static final String SYS_TABLE = "sys";
    public static final String MONITOR_TABLE = "mon";
    public static final String UPGRADE_TABLE = "upgrade";
    public static final String TEMP_TABLE = "temp";
    public static final String CONFIG_ADDRESS_KEY = "address";
    public static final String CONFIG_OLDADDRESS_KEY = "oldaddress";
    public static final String CONFIG_DB_KEY = "db";
    public static final String DELETE_DB = "delete";
    public static final String CONFIG_SHARDCHANNEL_KEY = "shardchannel";
    public static final String CONFIG_SHARDCHANNELSTATUS_KEY = "shardchannelstatus";
    public static final String CONFIG_UPGRADESTATUS_KEY = "upgradestatus";
    public static final String KEY_SEPARATOR = "-";
    public static final String ADDRESS_SEPARATOR = ":";
    public static final String SHARD_NORMAL_STATUS = "0";
    public static final String SHARD_START_STATUS = "1";
    public static final String SHARD_STARTMESSAGE_STATUS = "2";
    public static final String SHARD_COLLECT_STATUS = "3";
    public static final String SHARD_MOVE_STATUS = "4";
    public static final String SHARD_MOVEMESSAGE_STATUS = "5";
    public static final String SHARD_END_STATUS = "6";
    public static final String SHARD_REPLACE_STATUS = "R";
    public static final int SHARD_WAITTIME = 10;
    public static final int POOL_MAXACTIVE = 1000;
    public static final int POOL_MAXIDLE = 100;
    public static final int POOL_MAXWAIT = 10000;
    public static final int POOL_TIMEOUT = 10000;
    public static final int POOL_MINIDLE = 0;
    public static final boolean POOL_TESTONBORROW = false;
    public static final String STRING_TYPE = "string";
    public static final String LIST_TYPE = "list";
    public static final String SET_TYPE = "set";
    public static final String HASH_TYPE = "hash";
    public static final String ZSET_TYPE = "zset";
    public static final String NONE_TYPE = "none";
    public static final String FILE_SEPARATOR = "|";
    public static final String FILE_NAME = "DataCenter.xml";
    public static final String FILE_CONFIGADDRESS = "configAddress";
    public static final String FILE_INITADDRESS = "initAddress";
    public static final String FILE_ADDADDRESS = "addAddress";
    public static final String FILE_UPDATEADDRESS = "updateAddress";
    public static final String FILE_MASTER = "master";
    public static final String FILE_SLAVE = "slave";
    public static final String FILE_ADDCONFIG = "addConfig";
    public static final String FILE_ADDCONFIG_MOVETHREAD = "moveThread";
    public static final String FILE_ADDCONFIG_DELETETHREAD = "deleteThread";
    public static final String FILE_THRIFTCONFIG = "thriftConfig";
    public static final String FILE_THRIFTCONFIG_PORT = "port";
    public static final String USERSTATUS_CONTINUE = "continue";
    public static final String ERROR_PARAM = "param error";
    public static final String ERROR_CONNECTION = "connection fail";
    public static final String ERROR_ADDRESSPARAM = "address param error";
    public static final String MONITOR_SEPARATOR = "-";
    public static final String MONITOR_LIMIT = "80";
    public static final String MONITOR_STATUS_MASTER = "0";
    public static final String MONITOR_STATUS_SLAVE = "1";
    public static final String MONITOR_INIT_IPPORT = "0.0.0.0:0000";
    public static final int SCAN_COUNT = 10000;
}