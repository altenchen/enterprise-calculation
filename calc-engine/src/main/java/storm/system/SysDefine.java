package storm.system;

public final class SysDefine {
    public static final String DB_CACHE_FLUSH_TIME_SECOND = "db.cache.flushtime";

    public static final String KAFKA_ZOOKEEPER_SERVERS_KEY = "kafka.zookeeper.servers";

    public static final String KAFKA_ZOOKEEPER_PORT_KEY = "kafka.zookeeper.port";

    public static final String KAFKA_ZOOKEEPER_PATH_KEY = "kafka.zookeeper.path";

    /**
     * Kafka 经纪人及监听的端口
     */
    public static final String KAFKA_BOOTSTRAP_SERVERS_KEY = "kafka.bootstrap.servers";

    /**
     * 车辆实时数据 kafka 消费主题
     */
    public static final String KAFKA_CONSUMER_VEHICLE_REALTIME_DATA_TOPIC = "kafka.consumer.vehicle_realtime_data.topic";

    /**
     * 车辆实时数据 kafka 消费组
     */
    public static final String KAFKA_CONSUMER_VEHICLE_REALTIME_DATA_GROUP = "kafka.consumer.vehicle_realtime_data.group";

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

    public static final String CODE = "code";


    /**
     * 告警消息 kafka 输出 topic
     */
    public static final String VEHICLE_ALARM_TOPIC = "kafka.producer.vehicle_alarm.topic";

    /**
     * 围栏告警
     */
    public static final String KAFKA_PRODUCER_VEHICLE_FENCE_ALARM_TOPIC = "kafka.producer.vehicle_fence_alarm.topic";

    /**
     * 触发平台报警开始需要的连续次数
     */
    public static final String ALARM_START_TRIGGER_CONTINUE_COUNT = "alarm.start.trigger.continue.count";

    /**
     * 触发平台报警开始需要的持续时长
     */
    public static final String ALARM_START_TRIGGER_TIMEOUT_MILLISECOND = "alarm.start.trigger.timeout.millisecond";

    /**
     * 每隔多少时间推送一次,默认一分钟，60000毫秒。如果负数或者0代表实时推送, 单位秒.
     */
    public static final String ES_SEND_TIME = "es.send.time";

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
     * 规则覆盖, 默认为default
     */
    public static final String RULE_OVERRIDE = "rule.override";

    /**
     * 未定位开始时间阈值
     */
    public static final String GPS_JUDGE_TIME = "gps.judge.time";

    public static class Redis {
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

    }

    /**
     * 电子围栏规则ID-先定死，后面可能会改表结构，到时候这个ID需要从数据库读取
     */
    public static final String FENCE_INSIDE_EVENT_ID = "642acb18-70ae-4c8f-8970-222e61b1c6cd";
    public static final String FENCE_OUTSIDE_EVENT_ID = "8aaf5a62-b541-478e-88e7-405a7727867e";
    public static final String FENCE_AREA_ID = "f0837fa4-d94b-4954-aaf5-d5c9c6317f1b";
}
