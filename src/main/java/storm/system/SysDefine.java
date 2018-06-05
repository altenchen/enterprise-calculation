package storm.system;

public final class SysDefine {
	//接收实时数据TOPIC
    public static String VEH_REALINFO_DATA_TOPIC ;

    //Zookeeper根目录
    public static String ZKROOT;

    //车辆实时数据组ID
    public static String VEH_REALINFO_GROUPID;

  //kafka数据消费者地址
    public static String ZK_HOSTS;
    
    //kafka数据消费者地址
    public static String BROKER_HOSTS;

    //Zookeeper端口
    public static int ZKPORT;

    //Zookeeper地址
    public static String ZKSERVERS;

    //APP操作，平台到STORM的TOPIC名称
    public static String VEH_TEST_OPTION_TOPIC;

    //APP操作，平台到STORM的消息组ID
    public static String VEH_TEST_GROUPID;

    //错误报文TOPIC
    public static String ERROR_DATA_TOPIC;
    
    //平台注册报文TOPIC
	public static String PLAT_REG_TOPIC;

    //错误报文组名
    public static String ERROR_DATA_GROUPID;
    
    //错误报文组名
	public static String PLAT_REG_GROUPID;
    //
    public static final String HISTORY = "history";
    
    //车辆实时所有数据
    public static final String SYNC_REALINFO_STORE = "vehrealinfostore";
    //告警
    public static final String VEH_ALARM = "vehalarm";
    
    //围栏告警
	public static final String FENCE_ALARM = "fencealarm";
	
	//故障发送kafkaStream id
	public static final String FAULT_STREAM = "faultstreamId";
	//雅安发送
	public static final String YAACTION_NOTICE = "yanotice";
	 //同步 es消息发送
	public static final String SYNES_NOTICE = "synesnotice";
	//
    public static final String VEH_ALARM_REALINFO_STORE = "vehalarmrealinfostore";

    //实时数据 kafka stream id
    public static final String REALINFO_SPOUT_ID = "realinfospoutid";

    //
    public static final String VEHTESTCMD_SPOUT_ID = "vehtestcmdspoutid";

    //
    public static final String VEHTESTCMD_BOLT_ID = "vehtestcmdboltid";

    //
    public static final String REDIS_SPOUT_ID = "redisspoutid";

    // 预处理 boltid
    public static final String CHECKFILTER_BOLT_ID = "checkfilterboltid";

    //
    public static final String SPLIT_GROUP = "splitgroup";

    //
    public static final String VID = "VID";

    //
    public static final String ALARM_BOLT_ID = "alarmboltid";

    //
    public static final String REDIS_DATACONST = "redisdataconst";

    //
    public static final String KAFKASEND_BOLT_ID = "kafkasendboltid";

    //
    public static final String TEST_VEH_ACK = "testvehack";

    //
    public static final String TEST_VEH_CMD = "testvehcmd";

    //
    public static final String ERRORDATA_SPOUT_ID = "errordataspoutid";

    //
    public static final String INIT_CMD = "INIT";

    //
    public static final String LOGIN_CMD = "LOGIN";

    //
    public static final String REALTIME_CMD = "REALTIME";

    //
    public static final String HISTORY_CMD = "HISTORY";

    //
    public static final String LOGOUT_CMD = "LOGOUT";

    //
    public static final String ALARM_CMD = "ALARM";

    //
    public static final String STATIC_CMD = "STATIC";

    //
    public static final String CHARGE_CMD = "CHARGE";

    //
    public static final String FULL_CMD = "FULL";

    //
    public static final String END_CMD = "END";

    public static final String ALARM_HISTORY="ALARMHISTORY";
    public static final String EVENT_RANDOM1_CMD="EVENT_RUN_RANDOM1LOW";
    public static final String EVENT_RANDOM2_CMD="EVENT_RUN_RANDOM2STOPD";
    public static final String EVENT_RANDOM3_CMD="EVENT_RUN_RANDOM3ACC20KM";
    public static final String EVENT_RANDOM4_CMD="EVENT_RUN_RANDOM4ACC25KM";
    public static final String EVENT_RANDOM5_CMD="EVENT_RUN_RANDOM5ACC30KM";
    public static final String EVENT_RANDOM6_CMD="EVENT_RUN_RANDOM6ACC35KM";
    //
    public static final String TERMINAL_FLAG = "terminalflag";

    /*-------------------------------标点符号-------------------------------------*/
    /** 空格 */
    public static final String SPACES = " ";
    /** 空字符串 */
    public static final String EMPTY = "";
    /** 逗号 */
    public static final String COMMA = ",";
    /** 句号 */
    public static final String PERIOD = ".";
    /** 下划线 */
    public static final String UNDERLINE = "_";
    /** 冒号 */
    public static final String COLON = ":";
    /** 换行符 */
    public static final String NEWLINE = "\r\n";
    /** 反斜杠 */
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
    public final static String SUBMIT = "SUBMIT";
    /** 指令类型 原始报文 */
    public final static String PACKET = "PACKET";

	/*-------------------------------指令-------------------------------------*/
    /** 消息类型 */
    public static final String MESSAGETYPE = "MESSAGETYPE";
    /** 时间 */
    public static final String TIME = "TIME";
    /** 未知 */
    public static final String UNKNOW = "UNKNOW";
  /** 指令类型 终端注册消息 */
    public static final String LOGIN = "LOGIN";
    /** 指令类型 实时信息上报 */
    public static final String REALTIME = "REALTIME";
    /** 指令类型 状态信息上报 */
    public static final String TERMSTATUS = "TERMSTATUS";
    /** 指令类型 补发信息上报 */
    public static final String HISTORYDATA = "HISTORY";
    /** 指令类型 车辆运行状态 */
    public static final String CARSTATUS = "CARSTATUS";
    /** 指令类型 租赁点更新数据 */
    public static final String RENTALSTATION = "RENTALSTATION";
    /** 指令类型 租赁点更新数据 */
    public static final String CHARGESTATION = "CHARGESTATION";
    /** 指令类型 租赁数据 */
    public static final String RENTCAR = "RENTCAR";
    /** 指令类型 充电设施数据 */
    public static final String CHARGE = "CHARGE";
    /** 是否有告警 */
    public static final String ISALARM = "10001";
    /** 定时任务关键字 */
    public static final String ISONLINE = "10002";
    /** 是否充电 */
    public static final String ISCHARGE = "10003";
    /** 定时任务关键字 */
    public static final String MILEAGE = "10004";
    /** 定时任务关键字，当前总里程 */
    public static final String TOTAL_MILEAGE = "2202";
    /** 定时任务关键字 */
    public static final String ONLINEUTC = "10005";
    /** 定时任务关键字 */
    public static final String ALARMUTC = "ALARMUTC";
    /** 在线 */
    public static final String ONLINE_COUNT = "online_count";
    /** 行驶在线 */
    public static final String RUNNING_ONLINE = "running_online";
    /** 停止在线 */
    public static final String STOP_ONLINE = "stop_online";
    /** 车辆总数 */
    public static final String CAR_TOTAL = "car_total";
    /** 车辆数 */
    public static final String CAR_COUNT = "car_count";
    /** 今日活跃车辆数 */
    public static final String CAR_ACTIVE_COUNT = "car_active";
    /** 监控车辆数 */
    public static final String MONITOR_CAR_TOTAL = "monitor_car_count";
    /** 充电车辆数 */
    public static final String CHARGE_CAR_COUNT = "charge_car_count";
    /** 故障车辆数 */
    public static final String FAULT_COUNT = "fault_count";
    /** 总里程 */
    public static final String MILEAGE_TOTAL = "mileage_total";
    /** 在线比例 */
    public static final String ONLINE_RATIO = "online_ratio";
    /** 车辆总数 */
    public static final String VEHICLE_TOTAL = "vehicle_total";
    /** 指令类型 */
    public static final String ISFILTER = "ISFILTER";
    
    public static final String QUICK_BOLT_ID = "quickboltid";
    public static final String SUPPLY_GROUP="supplyGroup";
	public static final String FENCE_BOLT_ID = "fenceboltid";
	public static final String FENCE_GROUP = "fenceGroup";
	public static final String FAULT_BOLT_ID = "faultboltid";
	public static final String FAULT_GROUP = "faultGroup";
	/** 接收告警传送过来的同步es streamId分组 **/
	public static final String SYNES_BOLT_ID = "synesboltid";
	public static final String SYNES_GROUP = "synesGroup";
	public static final String YAACTION_BOLT_ID = "yaactionboltid";
	public static final String YAACTION_GROUP = "yaactionGroup";
    public static final String DIRECT="direct";//直连
    public static final String FORWARD="forward";//转发
	public static final String CODE = "code";
	public static final String REG_SPOUT_ID = "regpoutid";
	public static final String REG_STREAM_ID = "regStreamId";
	/** 接收传送过来的同步es streamId分组 **/
	public static final String CUS_NOTICE_BOLT_ID = "cusnoticeboltid";
	public static final String CUS_NOTICE_GROUP = "cusnoticeGroup";
	public static final String CUS_NOTICE = "cusnotice";
}
