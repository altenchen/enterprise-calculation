package storm.system;

import java.util.List;

public final class SysDefine {
    public static final String TOPOLOGY_NAME = "topology.name";

    public static final String DB_CACHE_FLUSH_TIME_SECOND = "db.cache.flushtime";

    public static final String KAFKA_ZOOKEEPER_SERVERS_KEY = "kafka.zookeeper.servers";

    public static final String KAFKA_ZOOKEEPER_PORT_KEY = "kafka.zookeeper.port";

    public static final String KAFKA_ZOOKEEPER_PATH_KEY = "kafka.zookeeper.path";

    /**
     * Kafka 经纪人及监听的端口
     */
    public static final String KAFKA_BOOTSTRAP_SERVERS_KEY = "kafka.bootstrap.servers";
    
    /**
     * 车辆原始报文 kafka 消费主题
     */
    public static final String KAFKA_CONSUMER_VEHICLE_PACKET_DATA_TOPIC = "kafka.consumer.vehicle_packet_data.topic";

    /**
     * 车辆原始报文 kafka 消费组
     */
    public static final String KAFKA_CONSUMER_VEHICLE_PACKET_DATA_GROUP = "kafka.consumer.vehicle_packet_data.group";

    /**
     * 车辆实时数据 kafka 消费主题
     */
    public static final String KAFKA_CONSUMER_VEHICLE_REALTIME_DATA_TOPIC = "kafka.consumer.vehicle_realtime_data.topic";

    /**
     * 车辆实时数据 kafka 消费组
     */
    public static final String KAFKA_CONSUMER_VEHICLE_REALTIME_DATA_GROUP = "kafka.consumer.vehicle_realtime_data.group";

    /**
     * 车辆注册通知 kafka 消费主题
     */
    public static final String KAFKA_CONSUMER_VEHICLE_REGISTER_DATA_TOPIC = "kafka.consumer.vehicle_register_data.topic";

    /**
     * 车辆注册通知 kafka 消费组
     */
    public static final String KAFKA_CONSUMER_VEHICLE_REGISTER_DATA_GROUP = "kafka.consumer.vehicle_register_data.group";

    /**
     * kafka 依赖的 zookeeper 集群主机
     */
    public static List<String> KAFKA_ZOOKEEPER_SERVERS;

    /**
     * kafka 依赖的 zookeeper 集群端口
     */
    public static int KAFKA_ZOOKEEPER_PORT;

    /**
     * kafka 依赖的 zookeeper 集群
     */
    public static String KAFKA_ZOOKEEPER_HOSTS;

    /**
     * kafka 元数据存储在 zookeeper 的路径
     */
    public static String KAFKA_ZOOKEEPER_PATH;

    /**
     * kafka 经纪人及监听的端口, 多个经纪人之间用英文逗号隔开.
     */
    public static String KAFKA_BOOTSTRAP_SERVERS;

    /**
     * 车辆原始报文 kafka 消费主题
     */
    public static String ERROR_DATA_TOPIC;

    /**
     * 车辆原始报文 kafka 消费组
     */
    public static String ERROR_DATA_GROUPID;

    /**
     * 车辆实时数据 kafka 消费主题
     */
    public static String VEH_REALINFO_DATA_TOPIC;

    /**
     * 车辆实时数据 kafka 消费组
     */
    public static String VEH_REALINFO_GROUPID;

    /**
     * 车辆注册通知 kafka 消费主题
     */
    public static String PLAT_REG_TOPIC;

    /**
     * 车辆注册通知 kafka 消费组
     */
    public static String PLAT_REG_GROUPID;


    //
    public static final String SPLIT_GROUP = "splitgroup";

    /*-------------------------------标点符号-------------------------------------*/
    /**
     * 空格
     */
    public static final String SPACES = " ";
    /**
     * 空字符串
     */
    public static final String EMPTY = "";
    /**
     * 逗号
     */
    public static final String COMMA = ",";
    /**
     * 句号
     */
    public static final String PERIOD = ".";
    /**
     * 下划线
     */
    public static final String UNDERLINE = "_";
    /**
     * 冒号
     */
    public static final String COLON = ":";
    /**
     * 换行符
     */
    public static final String NEWLINE = "\r\n";
    /**
     * 反斜杠
     */
    public static final String BACKSLASH = "/";

    /*-------------------------------内部协议常量-------------------------------------*/
    public final static String COMMAND = "command";// 原始指令
    public final static String HEAD = "head";// 包头
    public final static String SEQ = "seq";// 业务序列号
    public final static String MACID = "macid";// 车辆标识
    public final static String CHANNEL = "channel";// 通道
    public final static String MTYPE = "mtype";// 类型
    public final static String CONTENT = "content";// 具体内容
    public final static String MSGID = "msgid";// 消息服务器id
    public final static String UUID = "uuid";// 指令唯一标识uuid
    public final static String VIN = "VIN";// 车辆VIN
    public final static String PTYPE = "ptype";// 插件类型

    public final static String OEMCODE = "oecode"; // OEMCODE
    public final static String PLATECOLORID = "platecolorid"; // 车牌颜色ID
    public final static String TID = "tid"; // 终端ID
    public final static String VEHICLENO = "vehicleno"; // 车牌号

    /*-------------------------------指令-------------------------------------*/
    /**
     * 指令类型 租赁数据
     */
    public static final String RENTCAR = "RENTCAR";
    /**
     * 指令类型 充电设施数据
     */
    public static final String CHARGE = "CHARGE";
    /**
     * 是否有告警, 1-有告警 2- 无告警
     */
    public static final String IS_ALARM = "10001";
    /**
     * 是否充电
     */
    public static final String IS_CHARGE = "10003";
    /**
     * 定时任务关键字
     */
    public static final String ONLINE_UTC = "10005";
    /**
     * 定时任务关键字
     */
    public static final String ALARMUTC = "ALARMUTC";
    /**
     * 在线
     */
    public static final String ONLINE_COUNT = "online_count";
    /**
     * 行驶在线
     */
    public static final String RUNNING_ONLINE = "running_online";
    /**
     * 停止在线
     */
    public static final String STOP_ONLINE = "stop_online";
    /**
     * 车辆总数
     */
    public static final String CAR_TOTAL = "car_total";
    /**
     * 车辆数
     */
    public static final String CAR_COUNT = "car_count";
    /**
     * 监控车辆数
     */
    public static final String MONITOR_CAR_TOTAL = "monitor_car_count";
    /**
     * 充电车辆数
     */
    public static final String CHARGE_CAR_COUNT = "charge_car_count";
    /**
     * 故障车辆数
     */
    public static final String FAULT_COUNT = "fault_count";
    /**
     * 总里程
     */
    public static final String MILEAGE_TOTAL = "mileage_total";

    public static final String FENCE_GROUP = "fenceGroup";
    public static final String SYNES_GROUP = "synesGroup";
    public static final String CODE = "code";


    /**
     * 告警消息 kafka 输出 topic
     */
    public static final String KAFKA_TOPIC_ALARM = "kafka.producer.vehicle_alarm.topic";

    /**
     * HBase 车辆报警状态存储 kafka 输出 topic
     */
    public static final String KAFKA_TOPIC_ALARM_STORE = "kafka.producer.vehicle_alarm_store.topic";

    /**
     * 围栏告警
     */
    public static final String KAFKA_PRODUCER_VEHICLE_FENCE_ALARM_TOPIC = "kafka.producer.vehicle_fence_alarm.topic";

    /**
     * AlarmBolt, 连续多少条报警才发送通知
     */
    public static final String ALARM_CONTINUE_COUNTS = "alarm.continue.counts";

    /**
     * 每隔多少时间推送一次,默认一分钟，60000毫秒。如果负数或者0代表实时推送, 单位秒.
     */
    public static final String ES_SEND_TIME = "es.send.time";

    /**
     * kafka 发往 ElasticSearch 的 topic
     */
    public static final String KAFKA_TOPIC_ES_STATUS = "kafka.producer.elastic_search_status.topic";

    /**
     * 通知消息 kafka 输出 topic
     */
    public static final String KAFKA_TOPIC_NOTICE = "kafka.producer.vehicle_notice.topic";

    /**
     * 触发CAN故障需要的连续帧数
     */
    public static final String NOTICE_CAN_FAULT_TRIGGER_CONTINUE_COUNT = "notice.can.fault.trigger.continue.count";
    /**
     * 触发CAN故障需要的持续时长
     */
    public static final String NOTICE_CAN_FAULT_TRIGGER_TIMEOUT_MILLISECOND = "notice.can.fault.trigger.timeout.millisecond";
    /**
     * 触发CAN正常需要的连续帧数
     */
    public static final String NOTICE_CAN_NORMAL_TRIGGER_CONTINUE_COUNT = "notice.can.normal.trigger.continue.count";
    /**
     * 触发CAN正常需要的持续时长
     */
    public static final String NOTICE_CAN_NORMAL_TRIGGER_TIMEOUT_MILLISECOND = "notice.can.normal.trigger.timeout.millisecond";

    /**
     * 是否启用时间异常通知规则
     */
    public static final String NOTICE_TIME_ENABLE = "notice.time.enable";

    /**
     * 时间数值异常范围
     */
    public static final String NOTICE_TIME_RANGE_ABS_MILLISECOND = "notice.time.range.abs.millisecond";

    /**
     * 规则覆盖, 默认为default
     */
    public static final String RULE_OVERRIDE = "rule.override";

    /**
     * 默认规则
     */
    public static final String RULE_OVERRIDE_VALUE_DEFAULT = "default";

    /**
     * 吉利规则
     */
    public static final String RULE_OVERRIDE_VALUE_JILI = "jili";

    /**
     * soc过低阈值
     */
    public static final String LT_ALARM_SOC = "lt.alarm.soc";

    /**
     * soc故障判断时间阈值
     */
    public static final String SOC_FAULT_JUDGE_TIME = "notice.soc.fault.trigger.timeout.millisecond";
    /**
     * soc正常判断时间阈值
     */
    public static final String SOC_NORMAL_JUDGE_TIME = "notice.soc.normal.trigger.timeout.millisecond";
    /**
     * soc故障帧数判断
     */
    public static final String SOC_FAULT_JUDGE_NO = "notice.soc.fault.trigger.continue.count";
    /**
     * soc正常帧数判断
     */
    public static final String SOC_NORMAL_JUDGE_NO = "notice.soc.normal.trigger.continue.count";


    /**
     * 未定位开始帧数判断
     */
    public static final String GPS_NOVALUE_CONTINUE_NO = "gps.novalue.continue.no";
    /**
     * 未定位结束帧数判断
     */
    public static final String GPS_HASVALUE_CONTINUE_NO = "gps.hasvalue.continue.no";
    /**
     * 未定位开始时间阈值
     */
    public static final String GPS_JUDGE_TIME = "gps.judge.time";

    public static class Redis {
        /**
         * Redis 地址
         */
        public static final String HOST = "redis.host";
        /**
         * Redis 端口
         */
        public static final String PORT = "redis.port";
        /**
         * Redis 密码
         */
        public static final String PASSWORD = "redis.pass";
        /**
         * Redis 最大连接数
         */
        public static final String JEDIS_POOL_MAX_TOTAL = "redis.maxActive";
        /**
         * Redis 最大空闲数
         */
        public static final String JEDIS_POOL_MAX_IDLE = "redis.maxIdle";
        /**
         * Redis 最长等待时间(毫秒)
         */
        public static final String JEDIS_POOL_MAX_WAIT_MILLISECOND = "redis.maxWait";
        /**
         * Redis 超时时间(毫秒)
         */
        public static final String TIMEOUT = "redis.timeOut";

    }
}
