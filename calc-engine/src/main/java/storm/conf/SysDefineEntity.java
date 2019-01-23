package storm.conf;

/**
 * 系统运行参数, 通过反射从配置文件sysDefine.properties加载, 属性必须有set方法, 否则无法正确设置值.<br/>
 * <p>所有properties中的key包含. _符号的都转换为首字母大写， 具体映射解析 ConfigUtils.keyConvertAttributeName私有方法</p>
 * <p>示例</p>
 * <ul>
 * <li>stormWorkerNo 对应 storm.worker.no</li>
 * <li>stormKafkaSpoutNo 对应 storm.kafka.spout.no</li>
 * <li>kafkaProducerVehicleNoticeTopic 对应 kafka.producer.vehicle_notice.topic</li>
 * </ul>
 *
 * @author xuzhijie
 * @see storm.util.ConfigUtils;
 */
@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class SysDefineEntity {
    // ############################################################################################
    //                                        storm 配置
    // ############################################################################################
    /**
     * storm worker 数量, 请配置成与集群 supervisor 工作节点数量一致.
     */
    private int stormWorkerNo = 2;
    /**
     * storm worker 每个最大占用内存
     */
    private int stormWorkerHeapMemoryMb = 5440;
    /**
     * spout 并行度基准系数
     */
    private int stormKafkaSpoutNo = 1;
    /**
     * bolt 并行度基准系数
     */
    private int stormKafkaBoltNo = 2;
    /**
     * 拓扑名称
     */
    private String topologyName = null;

    // ############################################################################################
    //                                        kafka 配置
    // ############################################################################################
    /**
     * kafka 依赖的 zookeeper 集群, 只有使用 kafka 0.8.2 的 compatibility 版本才使用.
     */
    private String kafkaZookeeperServers = null;
    /**
     * kafka 依赖的 zookeeper 端口, 只有使用 kafka 0.8.2 的 compatibility 版本才使用.
     */
    private int kafkaZookeeperPort = 2181;
    /**
     * kafka 元数据存储在 zookeeper 的路径, 只有使用 kafka 0.8.2 的 compatibility 版本才使用.
     */
    private String kafkaZookeeperPath = null;
    /**
     * kafka 经纪人及监听的端口, 多个经纪人之间用英文逗号隔开.
     */
    private String kafkaBootstrapServers = null;

    private String kafkaConsumerVehicleRealtimeDataTopic = null;
    private String kafkaConsumerVehicleRealtimeDataGroup = null;

    /**
     * 车辆报警 topic, 下游AlarmService依赖, 请保持一致.
     */
    private String kafkaProducerVehicleAlarmTopic = "SYNC_REALTIME_ALARM";
    /**
     * 围栏告警, 下游AlarmService依赖, 请保持一致.
     */
    private String kafkaProducerVehicleFenceAlarmTopic = "FENCE_ALARM_TOPIC";
    /**
     * 车辆通知 topic, 下游AlarmService依赖, 请保持一致.
     */
    private String kafkaProducerVehicleNoticeTopic = "notice_topic";

    // ############################################################################################
    //                                        redis 配置
    // ############################################################################################
    /**
     * redis 连接地址
     */
    private String redisHost = null;
    /**
     * redis 端口
     */
    private int redisPort = 6379;
    /**
     * redis 密码
     */
    private String redisPass = null;
    /**
     * redis 最大连接数
     */
    private int redisMaxActive = 1000;
    /**
     * redis 最大空闲数
     */
    private int redisMaxIdle = 100;
    /**
     * redis 最长等待时间(毫秒)
     */
    private int redisMaxWait = 300000;
    /**
     * redis 超时时间(毫秒)
     */
    private int redisTimeOut = 300000;
    /**
     * 获取 redis 预处理、预警间隔时间(毫秒)
     */
    private int redisTimeInterval = 300;
    /**
     * 定时更新redis间隔时间(毫秒)
     */
    private int redisListenInterval = 300;
    private int redisTotalInterval = 180;

    // ############################################################################################
    //                                        ctfo 配置
    // ############################################################################################
    /**
     * 分布式 redis 地址
     */
    private String ctfoCacheHost = null;
    /**
     * 分布式 redis 端口
     */
    private int ctfoCachePort = 6379;
    /**
     * 分布式 redis 库名称
     */
    private String ctfoCacheDB = null;
    /**
     * 分布式 redis 表名
     */
    private String ctfoCacheTable = null;

    // ############################################################################################
    //                                        关系数据库 配置
    // ############################################################################################
    /**
     * 驱动类
     */
    private String jdbcDriver = "com.mysql.cj.jdbc.Driver";
    /**
     * 连接字符串
     */
    private String jdbcUrl = null;
    /**
     * 数据库账号
     */
    private String jdbcUsername = null;
    /**
     * 数据库密码
     */
    private String jdbcPassword = null;
    /**
     * 查询数据库间隔(秒)
     */
    private int dbCacheFlushtime = 360;

    // ############################################################################################
    //                                        通信超时 配置
    // ############################################################################################
    /**
     * 多长时间算是离线, 单位秒.
     */
    private int redisOfflineTime = 600;
    /**
     * 多长时间算是停止, 单位秒
     */
    private int redisOfflineStopTime = 180;


    // ############################################################################################
    //                                        大屏展示 配置
    // ############################################################################################
    /**
     * 多长时间保存一次数据到 redis, 单位秒
     */
    private int redisMonitorTime = 600;
    /**
     * 统计数据的线程池大小
     */
    private int statThreadNo = 30;

    // ############################################################################################
    //                                        电子围栏 配置
    // ############################################################################################

    private double fenceCoordinateCoefficient = 100000;
    /**
     * 电子围栏图形外部缓冲区
     */
    private double fenceShapeBufferOutsideMeter = 50d;
    /**
     * 电子围栏图形内部缓冲区
     */
    private double fenceShapeBufferInsideMeter = 50d;
    /**
     * 电子围栏定位最大距离差
     */
    private double fenceCoordinateDistanceMaxMeter = 1000d;

    /**
     * 触发电子围栏事件开始需要的连续次数 - 驶入事件
     */
    private int fenceEventDriveInsideStartTriggerContinueCount = 2;
    /**
     * 触发电子围栏事件开始需要的持续时长 - 驶入事件
     */
    private long fenceEventDriveInsideStartTriggerTimeoutMillisecond = 0;
    /**
     * 触发电子围栏事件结束需要的连续次数 - 驶入事件
     */
    private int fenceEventDriveInsideStopTriggerContinueCount = 2;
    /**
     * 触发电子围栏事件结束需要的持续时长 - 驶入事件
     */
    private long fenceEventDriveInsideStopTriggerTimeoutMillisecond = 0;

    /**
     * 触发电子围栏事件开始需要的连续次数 - 驶离事件
     */
    private int fenceEventDriveOutsideStartTriggerContinueCount = 2;
    /**
     * 触发电子围栏事件开始需要的持续时长 - 驶离事件
     */
    private long fenceEventDriveOutsideStartTriggerTimeoutMillisecond = 0;
    /**
     * 触发电子围栏事件结束需要的连续次数 - 驶离事件
     */
    private int fenceEventDriveOutsideStopTriggerContinueCount = 2;
    /**
     * 触发电子围栏事件结束需要的持续时长 - 驶离事件
     */
    private long fenceEventDriveOutsideStopTriggerTimeoutMillisecond = 0;


    // ############################################################################################
    //                                         企业通知 配置
    // ############################################################################################
    /**
     * CarNoticelBolt, 如果配置为2, 则进行一次全量数据扫描, 并将告警数据发送到kafka
     */
    private int redisClusterDataSyn = 1;
    /**
     * CarNoticelBolt, 多长时间检查一下是否离线
     */
    private int redisOfflineCheckTime = 90;
    /**
     * 触发平台报警开始需要的连续次数
     */
    private int alarmStartTriggerContinueCount = 3;
    /**
     * 触发平台报警开始需要的持续时长
     */
    private long alarmStartTriggerTimeoutMillisecond = 30000;
    /**
     * 触发平台报警结束需要的连续次数
     */
    private int alarmStopTriggerContinueCount = 3;
    /**
     * 触发平台报警结束需要的持续时长
     */
    private long alarmStopTriggerTimeoutMillisecond = 30000;
    /**
     * 是否启用异常用车通知 1启用，0关闭
     */
    private int sysAbnormalRule = 0;
    /**
     * 是否开启飞机的通知信息 1启用，0关闭
     */
    private int sysFlyRule = 0;
    /**
     * 是否开启上下线通知 1启用，0关闭
     */
    private int sysOnOffRule = 1;
    /**
     * 是否开启连续里程跳变通知 1启用，0关闭
     */
    private int sysMilehopRule = 1;
    /**
     * 是否启用车辆锁止变化通知 1启用，0关闭
     */
    private int sysCarLockStatusRule = 1;
    /**
     * 长期离线车辆判定时长
     */
    private long vehicleIdleTimeoutMillisecond = 86400000;

    /**
     * 是否启用点火熄火通知 1启用，0关闭
     */
    private int sysIgniteRule = 0;
    /**
     * 点火连续帧数
     */
    private int noticeIgniteTriggerContinueCount = 2;
    /**
     * 点火持续时长
     */
    private long noticeIgniteTriggerTimeoutMillisecond = 0;
    /**
     * 熄火连续帧数
     */
    private int noticeShutTriggerContinueCount = 1;
    /**
     * 熄火持续时长
     */
    private long noticeShutTriggerTimeoutMillisecond = 0;
    /**
     * 是否启用CAN监测通知
     */
    private int sysCanRule = 1;
    /**
     * 触发CAN故障需要的连续帧数
     */
    private int noticeCanFaultTriggerContinueCount = 7;
    /**
     * 触发CAN故障需要的持续时长
     */
    private long noticeCanFaultTriggerTimeoutMillisecond = 30000;
    /**
     * 触发CAN正常需要的连续帧数
     */
    private int noticeCanNormalTriggerContinueCount = 3;
    /**
     * 触发CAN正常需要的持续时长
     */
    private long noticeCanNormalTriggerTimeoutMillisecond = 30000;
    /**
     * 是否启用时间异常通知
     */
    private boolean noticeTimeEnable = true;
    /**
     * 时间数值异常范围
     */
    private long noticeTimeRangeAbsMillisecond = 600000;
    /**
     * 是否启用SOC过低通知
     */
    private boolean noticeSocLowEnable = true;
    /**
     * soc过低开始通知触发器, 小于等于阈值
     */
    private int noticeSocLowBeginTriggerThreshold = 10;
    /**
     * soc过低开始通知触发器, 连续帧数
     */
    private int noticeSocLowBeginTriggerContinueCount = 3;
    /**
     * soc过低开始通知触发器, 持续时长
     */
    private long noticeSocLowBeginTriggerTimeoutMillisecond = 30000;
    /**
     * soc过低结束通知触发器, 大于阈值
     */
    private int noticeSocLowEndTriggerThreshold = 10;
    /**
     * soc过低结束通知触发器, 连续帧数
     */
    private int noticeSocLowEndTriggerContinueCount = 1;
    /**
     * ssoc过低结束通知触发器, 持续时长
     */
    private long noticeSocLowEndTriggerTimeoutMillisecond = 0;
    /**
     * 是否启用SOC过高通知
     */
    private boolean noticeSocHighEnable = true;
    /**
     * soc过高开始通知触发器, 小于等于阈值
     */
    private int noticeSocHighBeginTriggerThreshold = 90;
    /**
     * soc过高开始通知触发器, 连续帧数
     */
    private int noticeSocHighBeginTriggerContinueCount = 3;
    /**
     * soc过高开始通知触发器, 持续时长
     */
    private long noticeSocHighBeginTriggerTimeoutMillisecond = 30000;
    /**
     * soc过高结束通知触发器, 大于阈值
     */
    private int noticeSocHighEndTriggerThreshold = 80;
    /**
     * soc过高结束通知触发器, 连续帧数
     */
    private int noticeSocHighEndTriggerContinueCount = 1;
    /**
     * ssoc过高结束通知触发器, 持续时长
     */
    private long noticeSocHighEndTriggerTimeoutMillisecond = 0;
    /**
     * 是否启用未定位通知
     */
    private int sysGpsRule = 1;
    /**
     * 触发定位故障需要的连续帧数
     */
    private int gpsNovalueContinueNo = 5;
    /**
     * 触发定位正常需要的连续帧数
     */
    private int gpsHasvalueContinueNo = 10;
    /**
     * 触发定位故障需要的持续时长(秒)
     */
    private int gpsJudgeTime = 60;
    /**
     * 厂商规则覆盖, 默认为default, 支持{jili}
     */
    private String ruleOverride = "jili";

    /**
     * 里程跳变数，单位是km, 2表示2公里
     */
    private int mileHopNum = 2;

    // region 自动生成的访问器

    public int getStormWorkerNo() {
        return stormWorkerNo;
    }

    public void setStormWorkerNo(final int stormWorkerNo) {
        this.stormWorkerNo = stormWorkerNo;
    }

    public int getStormWorkerHeapMemoryMb() {
        return stormWorkerHeapMemoryMb;
    }

    public void setStormWorkerHeapMemoryMb(final int stormWorkerHeapMemoryMb) {
        this.stormWorkerHeapMemoryMb = stormWorkerHeapMemoryMb;
    }

    public int getStormKafkaSpoutNo() {
        return stormKafkaSpoutNo;
    }

    public void setStormKafkaSpoutNo(final int stormKafkaSpoutNo) {
        this.stormKafkaSpoutNo = stormKafkaSpoutNo;
    }

    public int getStormKafkaBoltNo() {
        return stormKafkaBoltNo;
    }

    public void setStormKafkaBoltNo(final int stormKafkaBoltNo) {
        this.stormKafkaBoltNo = stormKafkaBoltNo;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(final String topologyName) {
        this.topologyName = topologyName;
    }

    public String getKafkaZookeeperServers() {
        return kafkaZookeeperServers;
    }

    public void setKafkaZookeeperServers(final String kafkaZookeeperServers) {
        this.kafkaZookeeperServers = kafkaZookeeperServers;
    }

    public int getKafkaZookeeperPort() {
        return kafkaZookeeperPort;
    }

    public void setKafkaZookeeperPort(final int kafkaZookeeperPort) {
        this.kafkaZookeeperPort = kafkaZookeeperPort;
    }

    public String getKafkaZookeeperPath() {
        return kafkaZookeeperPath;
    }

    public void setKafkaZookeeperPath(final String kafkaZookeeperPath) {
        this.kafkaZookeeperPath = kafkaZookeeperPath;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(final String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaConsumerVehicleRealtimeDataTopic() {
        return kafkaConsumerVehicleRealtimeDataTopic;
    }

    public void setKafkaConsumerVehicleRealtimeDataTopic(final String kafkaConsumerVehicleRealtimeDataTopic) {
        this.kafkaConsumerVehicleRealtimeDataTopic = kafkaConsumerVehicleRealtimeDataTopic;
    }

    public String getKafkaConsumerVehicleRealtimeDataGroup() {
        return kafkaConsumerVehicleRealtimeDataGroup;
    }

    public void setKafkaConsumerVehicleRealtimeDataGroup(final String kafkaConsumerVehicleRealtimeDataGroup) {
        this.kafkaConsumerVehicleRealtimeDataGroup = kafkaConsumerVehicleRealtimeDataGroup;
    }

    public String getKafkaProducerVehicleAlarmTopic() {
        return kafkaProducerVehicleAlarmTopic;
    }

    public void setKafkaProducerVehicleAlarmTopic(final String kafkaProducerVehicleAlarmTopic) {
        this.kafkaProducerVehicleAlarmTopic = kafkaProducerVehicleAlarmTopic;
    }

    public String getKafkaProducerVehicleFenceAlarmTopic() {
        return kafkaProducerVehicleFenceAlarmTopic;
    }

    public void setKafkaProducerVehicleFenceAlarmTopic(final String kafkaProducerVehicleFenceAlarmTopic) {
        this.kafkaProducerVehicleFenceAlarmTopic = kafkaProducerVehicleFenceAlarmTopic;
    }

    public String getKafkaProducerVehicleNoticeTopic() {
        return kafkaProducerVehicleNoticeTopic;
    }

    public void setKafkaProducerVehicleNoticeTopic(final String kafkaProducerVehicleNoticeTopic) {
        this.kafkaProducerVehicleNoticeTopic = kafkaProducerVehicleNoticeTopic;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(final String redisHost) {
        this.redisHost = redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(final int redisPort) {
        this.redisPort = redisPort;
    }

    public String getRedisPass() {
        return redisPass;
    }

    public void setRedisPass(final String redisPass) {
        this.redisPass = redisPass;
    }

    public int getRedisMaxActive() {
        return redisMaxActive;
    }

    public void setRedisMaxActive(final int redisMaxActive) {
        this.redisMaxActive = redisMaxActive;
    }

    public int getRedisMaxIdle() {
        return redisMaxIdle;
    }

    public void setRedisMaxIdle(final int redisMaxIdle) {
        this.redisMaxIdle = redisMaxIdle;
    }

    public int getRedisMaxWait() {
        return redisMaxWait;
    }

    public void setRedisMaxWait(final int redisMaxWait) {
        this.redisMaxWait = redisMaxWait;
    }

    public int getRedisTimeOut() {
        return redisTimeOut;
    }

    public void setRedisTimeOut(final int redisTimeOut) {
        this.redisTimeOut = redisTimeOut;
    }

    public int getRedisTimeInterval() {
        return redisTimeInterval;
    }

    public void setRedisTimeInterval(final int redisTimeInterval) {
        this.redisTimeInterval = redisTimeInterval;
    }

    public int getRedisListenInterval() {
        return redisListenInterval;
    }

    public void setRedisListenInterval(final int redisListenInterval) {
        this.redisListenInterval = redisListenInterval;
    }

    public int getRedisTotalInterval() {
        return redisTotalInterval;
    }

    public void setRedisTotalInterval(final int redisTotalInterval) {
        this.redisTotalInterval = redisTotalInterval;
    }

    public String getCtfoCacheHost() {
        return ctfoCacheHost;
    }

    public void setCtfoCacheHost(final String ctfoCacheHost) {
        this.ctfoCacheHost = ctfoCacheHost;
    }

    public int getCtfoCachePort() {
        return ctfoCachePort;
    }

    public void setCtfoCachePort(final int ctfoCachePort) {
        this.ctfoCachePort = ctfoCachePort;
    }

    public String getCtfoCacheDB() {
        return ctfoCacheDB;
    }

    public void setCtfoCacheDB(final String ctfoCacheDB) {
        this.ctfoCacheDB = ctfoCacheDB;
    }

    public String getCtfoCacheTable() {
        return ctfoCacheTable;
    }

    public void setCtfoCacheTable(final String ctfoCacheTable) {
        this.ctfoCacheTable = ctfoCacheTable;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(final String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(final String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getJdbcUsername() {
        return jdbcUsername;
    }

    public void setJdbcUsername(final String jdbcUsername) {
        this.jdbcUsername = jdbcUsername;
    }

    public String getJdbcPassword() {
        return jdbcPassword;
    }

    public void setJdbcPassword(final String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    public double getFenceShapeBufferOutsideMeter() {
        return fenceShapeBufferOutsideMeter;
    }

    public void setFenceShapeBufferOutsideMeter(final double fenceShapeBufferOutsideMeter) {
        this.fenceShapeBufferOutsideMeter = fenceShapeBufferOutsideMeter;
    }

    public double getFenceShapeBufferInsideMeter() {
        return fenceShapeBufferInsideMeter;
    }

    public void setFenceShapeBufferInsideMeter(final double fenceShapeBufferInsideMeter) {
        this.fenceShapeBufferInsideMeter = fenceShapeBufferInsideMeter;
    }

    public double getFenceCoordinateDistanceMaxMeter() {
        return fenceCoordinateDistanceMaxMeter;
    }

    public void setFenceCoordinateDistanceMaxMeter(final double fenceCoordinateDistanceMaxMeter) {
        this.fenceCoordinateDistanceMaxMeter = fenceCoordinateDistanceMaxMeter;
    }

    public int getDbCacheFlushtime() {
        return dbCacheFlushtime;
    }

    public void setDbCacheFlushtime(final int dbCacheFlushtime) {
        this.dbCacheFlushtime = dbCacheFlushtime;
    }

    public int getRedisOfflineTime() {
        return redisOfflineTime;
    }

    public void setRedisOfflineTime(final int redisOfflineTime) {
        this.redisOfflineTime = redisOfflineTime;
    }

    public int getRedisOfflineStopTime() {
        return redisOfflineStopTime;
    }

    public void setRedisOfflineStopTime(final int redisOfflineStopTime) {
        this.redisOfflineStopTime = redisOfflineStopTime;
    }

    public int getRedisMonitorTime() {
        return redisMonitorTime;
    }

    public void setRedisMonitorTime(final int redisMonitorTime) {
        this.redisMonitorTime = redisMonitorTime;
    }

    public int getStatThreadNo() {
        return statThreadNo;
    }

    public void setStatThreadNo(final int statThreadNo) {
        this.statThreadNo = statThreadNo;
    }

    public int getRedisClusterDataSyn() {
        return redisClusterDataSyn;
    }

    public void setRedisClusterDataSyn(final int redisClusterDataSyn) {
        this.redisClusterDataSyn = redisClusterDataSyn;
    }

    public int getRedisOfflineCheckTime() {
        return redisOfflineCheckTime;
    }

    public void setRedisOfflineCheckTime(final int redisOfflineCheckTime) {
        this.redisOfflineCheckTime = redisOfflineCheckTime;
    }

    public int getAlarmStartTriggerContinueCount() {
        return alarmStartTriggerContinueCount;
    }

    public void setAlarmStartTriggerContinueCount(final int alarmStartTriggerContinueCount) {
        this.alarmStartTriggerContinueCount = alarmStartTriggerContinueCount;
    }

    public long getAlarmStartTriggerTimeoutMillisecond() {
        return alarmStartTriggerTimeoutMillisecond;
    }

    public void setAlarmStartTriggerTimeoutMillisecond(final long alarmStartTriggerTimeoutMillisecond) {
        this.alarmStartTriggerTimeoutMillisecond = alarmStartTriggerTimeoutMillisecond;
    }

    public int getAlarmStopTriggerContinueCount() {
        return alarmStopTriggerContinueCount;
    }

    public void setAlarmStopTriggerContinueCount(final int alarmStopTriggerContinueCount) {
        this.alarmStopTriggerContinueCount = alarmStopTriggerContinueCount;
    }

    public long getAlarmStopTriggerTimeoutMillisecond() {
        return alarmStopTriggerTimeoutMillisecond;
    }

    public void setAlarmStopTriggerTimeoutMillisecond(final long alarmStopTriggerTimeoutMillisecond) {
        this.alarmStopTriggerTimeoutMillisecond = alarmStopTriggerTimeoutMillisecond;
    }

    public int getSysAbnormalRule() {
        return sysAbnormalRule;
    }

    public void setSysAbnormalRule(final int sysAbnormalRule) {
        this.sysAbnormalRule = sysAbnormalRule;
    }

    public int getSysFlyRule() {
        return sysFlyRule;
    }

    public void setSysFlyRule(final int sysFlyRule) {
        this.sysFlyRule = sysFlyRule;
    }

    public int getSysOnOffRule() {
        return sysOnOffRule;
    }

    public void setSysOnOffRule(final int sysOnOffRule) {
        this.sysOnOffRule = sysOnOffRule;
    }

    public int getSysMilehopRule() {
        return sysMilehopRule;
    }

    public void setSysMilehopRule(final int sysMilehopRule) {
        this.sysMilehopRule = sysMilehopRule;
    }

    public int getSysCarLockStatusRule() {
        return sysCarLockStatusRule;
    }

    public void setSysCarLockStatusRule(final int sysCarLockStatusRule) {
        this.sysCarLockStatusRule = sysCarLockStatusRule;
    }

    public long getVehicleIdleTimeoutMillisecond() {
        return vehicleIdleTimeoutMillisecond;
    }

    public void setVehicleIdleTimeoutMillisecond(final long vehicleIdleTimeoutMillisecond) {
        this.vehicleIdleTimeoutMillisecond = vehicleIdleTimeoutMillisecond;
    }

    public int getSysIgniteRule() {
        return sysIgniteRule;
    }

    public void setSysIgniteRule(final int sysIgniteRule) {
        this.sysIgniteRule = sysIgniteRule;
    }

    public int getNoticeIgniteTriggerContinueCount() {
        return noticeIgniteTriggerContinueCount;
    }

    public void setNoticeIgniteTriggerContinueCount(final int noticeIgniteTriggerContinueCount) {
        this.noticeIgniteTriggerContinueCount = noticeIgniteTriggerContinueCount;
    }

    public long getNoticeIgniteTriggerTimeoutMillisecond() {
        return noticeIgniteTriggerTimeoutMillisecond;
    }

    public void setNoticeIgniteTriggerTimeoutMillisecond(final long noticeIgniteTriggerTimeoutMillisecond) {
        this.noticeIgniteTriggerTimeoutMillisecond = noticeIgniteTriggerTimeoutMillisecond;
    }

    public int getNoticeShutTriggerContinueCount() {
        return noticeShutTriggerContinueCount;
    }

    public void setNoticeShutTriggerContinueCount(final int noticeShutTriggerContinueCount) {
        this.noticeShutTriggerContinueCount = noticeShutTriggerContinueCount;
    }

    public long getNoticeShutTriggerTimeoutMillisecond() {
        return noticeShutTriggerTimeoutMillisecond;
    }

    public void setNoticeShutTriggerTimeoutMillisecond(final long noticeShutTriggerTimeoutMillisecond) {
        this.noticeShutTriggerTimeoutMillisecond = noticeShutTriggerTimeoutMillisecond;
    }

    public int getSysCanRule() {
        return sysCanRule;
    }

    public void setSysCanRule(final int sysCanRule) {
        this.sysCanRule = sysCanRule;
    }

    public int getNoticeCanFaultTriggerContinueCount() {
        return noticeCanFaultTriggerContinueCount;
    }

    public void setNoticeCanFaultTriggerContinueCount(final int noticeCanFaultTriggerContinueCount) {
        this.noticeCanFaultTriggerContinueCount = noticeCanFaultTriggerContinueCount;
    }

    public long getNoticeCanFaultTriggerTimeoutMillisecond() {
        return noticeCanFaultTriggerTimeoutMillisecond;
    }

    public void setNoticeCanFaultTriggerTimeoutMillisecond(final long noticeCanFaultTriggerTimeoutMillisecond) {
        this.noticeCanFaultTriggerTimeoutMillisecond = noticeCanFaultTriggerTimeoutMillisecond;
    }

    public int getNoticeCanNormalTriggerContinueCount() {
        return noticeCanNormalTriggerContinueCount;
    }

    public void setNoticeCanNormalTriggerContinueCount(final int noticeCanNormalTriggerContinueCount) {
        this.noticeCanNormalTriggerContinueCount = noticeCanNormalTriggerContinueCount;
    }

    public long getNoticeCanNormalTriggerTimeoutMillisecond() {
        return noticeCanNormalTriggerTimeoutMillisecond;
    }

    public void setNoticeCanNormalTriggerTimeoutMillisecond(final long noticeCanNormalTriggerTimeoutMillisecond) {
        this.noticeCanNormalTriggerTimeoutMillisecond = noticeCanNormalTriggerTimeoutMillisecond;
    }

    public boolean isNoticeTimeEnable() {
        return noticeTimeEnable;
    }

    public void setNoticeTimeEnable(final boolean noticeTimeEnable) {
        this.noticeTimeEnable = noticeTimeEnable;
    }

    public long getNoticeTimeRangeAbsMillisecond() {
        return noticeTimeRangeAbsMillisecond;
    }

    public void setNoticeTimeRangeAbsMillisecond(final long noticeTimeRangeAbsMillisecond) {
        this.noticeTimeRangeAbsMillisecond = noticeTimeRangeAbsMillisecond;
    }

    public boolean isNoticeSocLowEnable() {
        return noticeSocLowEnable;
    }

    public void setNoticeSocLowEnable(final boolean noticeSocLowEnable) {
        this.noticeSocLowEnable = noticeSocLowEnable;
    }

    public int getNoticeSocLowBeginTriggerThreshold() {
        return noticeSocLowBeginTriggerThreshold;
    }

    public void setNoticeSocLowBeginTriggerThreshold(final int noticeSocLowBeginTriggerThreshold) {
        this.noticeSocLowBeginTriggerThreshold = noticeSocLowBeginTriggerThreshold;
    }

    public int getNoticeSocLowBeginTriggerContinueCount() {
        return noticeSocLowBeginTriggerContinueCount;
    }

    public void setNoticeSocLowBeginTriggerContinueCount(final int noticeSocLowBeginTriggerContinueCount) {
        this.noticeSocLowBeginTriggerContinueCount = noticeSocLowBeginTriggerContinueCount;
    }

    public long getNoticeSocLowBeginTriggerTimeoutMillisecond() {
        return noticeSocLowBeginTriggerTimeoutMillisecond;
    }

    public void setNoticeSocLowBeginTriggerTimeoutMillisecond(final long noticeSocLowBeginTriggerTimeoutMillisecond) {
        this.noticeSocLowBeginTriggerTimeoutMillisecond = noticeSocLowBeginTriggerTimeoutMillisecond;
    }

    public int getNoticeSocLowEndTriggerThreshold() {
        return noticeSocLowEndTriggerThreshold;
    }

    public void setNoticeSocLowEndTriggerThreshold(final int noticeSocLowEndTriggerThreshold) {
        this.noticeSocLowEndTriggerThreshold = noticeSocLowEndTriggerThreshold;
    }

    public int getNoticeSocLowEndTriggerContinueCount() {
        return noticeSocLowEndTriggerContinueCount;
    }

    public void setNoticeSocLowEndTriggerContinueCount(final int noticeSocLowEndTriggerContinueCount) {
        this.noticeSocLowEndTriggerContinueCount = noticeSocLowEndTriggerContinueCount;
    }

    public long getNoticeSocLowEndTriggerTimeoutMillisecond() {
        return noticeSocLowEndTriggerTimeoutMillisecond;
    }

    public void setNoticeSocLowEndTriggerTimeoutMillisecond(final long noticeSocLowEndTriggerTimeoutMillisecond) {
        this.noticeSocLowEndTriggerTimeoutMillisecond = noticeSocLowEndTriggerTimeoutMillisecond;
    }

    public boolean isNoticeSocHighEnable() {
        return noticeSocHighEnable;
    }

    public void setNoticeSocHighEnable(final boolean noticeSocHighEnable) {
        this.noticeSocHighEnable = noticeSocHighEnable;
    }

    public int getNoticeSocHighBeginTriggerThreshold() {
        return noticeSocHighBeginTriggerThreshold;
    }

    public void setNoticeSocHighBeginTriggerThreshold(final int noticeSocHighBeginTriggerThreshold) {
        this.noticeSocHighBeginTriggerThreshold = noticeSocHighBeginTriggerThreshold;
    }

    public int getNoticeSocHighBeginTriggerContinueCount() {
        return noticeSocHighBeginTriggerContinueCount;
    }

    public void setNoticeSocHighBeginTriggerContinueCount(final int noticeSocHighBeginTriggerContinueCount) {
        this.noticeSocHighBeginTriggerContinueCount = noticeSocHighBeginTriggerContinueCount;
    }

    public long getNoticeSocHighBeginTriggerTimeoutMillisecond() {
        return noticeSocHighBeginTriggerTimeoutMillisecond;
    }

    public void setNoticeSocHighBeginTriggerTimeoutMillisecond(final long noticeSocHighBeginTriggerTimeoutMillisecond) {
        this.noticeSocHighBeginTriggerTimeoutMillisecond = noticeSocHighBeginTriggerTimeoutMillisecond;
    }

    public int getNoticeSocHighEndTriggerThreshold() {
        return noticeSocHighEndTriggerThreshold;
    }

    public void setNoticeSocHighEndTriggerThreshold(final int noticeSocHighEndTriggerThreshold) {
        this.noticeSocHighEndTriggerThreshold = noticeSocHighEndTriggerThreshold;
    }

    public int getNoticeSocHighEndTriggerContinueCount() {
        return noticeSocHighEndTriggerContinueCount;
    }

    public void setNoticeSocHighEndTriggerContinueCount(final int noticeSocHighEndTriggerContinueCount) {
        this.noticeSocHighEndTriggerContinueCount = noticeSocHighEndTriggerContinueCount;
    }

    public long getNoticeSocHighEndTriggerTimeoutMillisecond() {
        return noticeSocHighEndTriggerTimeoutMillisecond;
    }

    public void setNoticeSocHighEndTriggerTimeoutMillisecond(final long noticeSocHighEndTriggerTimeoutMillisecond) {
        this.noticeSocHighEndTriggerTimeoutMillisecond = noticeSocHighEndTriggerTimeoutMillisecond;
    }

    public int getSysGpsRule() {
        return sysGpsRule;
    }

    public void setSysGpsRule(final int sysGpsRule) {
        this.sysGpsRule = sysGpsRule;
    }

    public int getGpsNovalueContinueNo() {
        return gpsNovalueContinueNo;
    }

    public void setGpsNovalueContinueNo(final int gpsNovalueContinueNo) {
        this.gpsNovalueContinueNo = gpsNovalueContinueNo;
    }

    public int getGpsHasvalueContinueNo() {
        return gpsHasvalueContinueNo;
    }

    public void setGpsHasvalueContinueNo(final int gpsHasvalueContinueNo) {
        this.gpsHasvalueContinueNo = gpsHasvalueContinueNo;
    }

    public int getGpsJudgeTime() {
        return gpsJudgeTime;
    }

    public void setGpsJudgeTime(final int gpsJudgeTime) {
        this.gpsJudgeTime = gpsJudgeTime;
    }

    public String getRuleOverride() {
        return ruleOverride;
    }

    public void setRuleOverride(final String ruleOverride) {
        this.ruleOverride = ruleOverride;
    }

    public int getMileHopNum() {
        return mileHopNum;
    }

    public void setMileHopNum(final int mileHopNum) {
        this.mileHopNum = mileHopNum;
    }

    public int getFenceEventDriveInsideStartTriggerContinueCount() {
        return fenceEventDriveInsideStartTriggerContinueCount;
    }

    public void setFenceEventDriveInsideStartTriggerContinueCount(int fenceEventDriveInsideStartTriggerContinueCount) {
        this.fenceEventDriveInsideStartTriggerContinueCount = fenceEventDriveInsideStartTriggerContinueCount;
    }

    public long getFenceEventDriveInsideStartTriggerTimeoutMillisecond() {
        return fenceEventDriveInsideStartTriggerTimeoutMillisecond;
    }

    public void setFenceEventDriveInsideStartTriggerTimeoutMillisecond(long fenceEventDriveInsideStartTriggerTimeoutMillisecond) {
        this.fenceEventDriveInsideStartTriggerTimeoutMillisecond = fenceEventDriveInsideStartTriggerTimeoutMillisecond;
    }

    public int getFenceEventDriveInsideStopTriggerContinueCount() {
        return fenceEventDriveInsideStopTriggerContinueCount;
    }

    public void setFenceEventDriveInsideStopTriggerContinueCount(int fenceEventDriveInsideStopTriggerContinueCount) {
        this.fenceEventDriveInsideStopTriggerContinueCount = fenceEventDriveInsideStopTriggerContinueCount;
    }

    public long getFenceEventDriveInsideStopTriggerTimeoutMillisecond() {
        return fenceEventDriveInsideStopTriggerTimeoutMillisecond;
    }

    public void setFenceEventDriveInsideStopTriggerTimeoutMillisecond(long fenceEventDriveInsideStopTriggerTimeoutMillisecond) {
        this.fenceEventDriveInsideStopTriggerTimeoutMillisecond = fenceEventDriveInsideStopTriggerTimeoutMillisecond;
    }

    public int getFenceEventDriveOutsideStartTriggerContinueCount() {
        return fenceEventDriveOutsideStartTriggerContinueCount;
    }

    public void setFenceEventDriveOutsideStartTriggerContinueCount(int fenceEventDriveOutsideStartTriggerContinueCount) {
        this.fenceEventDriveOutsideStartTriggerContinueCount = fenceEventDriveOutsideStartTriggerContinueCount;
    }

    public long getFenceEventDriveOutsideStartTriggerTimeoutMillisecond() {
        return fenceEventDriveOutsideStartTriggerTimeoutMillisecond;
    }

    public void setFenceEventDriveOutsideStartTriggerTimeoutMillisecond(long fenceEventDriveOutsideStartTriggerTimeoutMillisecond) {
        this.fenceEventDriveOutsideStartTriggerTimeoutMillisecond = fenceEventDriveOutsideStartTriggerTimeoutMillisecond;
    }

    public int getFenceEventDriveOutsideStopTriggerContinueCount() {
        return fenceEventDriveOutsideStopTriggerContinueCount;
    }

    public void setFenceEventDriveOutsideStopTriggerContinueCount(int fenceEventDriveOutsideStopTriggerContinueCount) {
        this.fenceEventDriveOutsideStopTriggerContinueCount = fenceEventDriveOutsideStopTriggerContinueCount;
    }

    public long getFenceEventDriveOutsideStopTriggerTimeoutMillisecond() {
        return fenceEventDriveOutsideStopTriggerTimeoutMillisecond;
    }

    public void setFenceEventDriveOutsideStopTriggerTimeoutMillisecond(long fenceEventDriveOutsideStopTriggerTimeoutMillisecond) {
        this.fenceEventDriveOutsideStopTriggerTimeoutMillisecond = fenceEventDriveOutsideStopTriggerTimeoutMillisecond;
    }

    public double getFenceCoordinateCoefficient() {
        return fenceCoordinateCoefficient;
    }

    public void setFenceCoordinateCoefficient(final double fenceCoordinateCoefficient) {
        this.fenceCoordinateCoefficient = fenceCoordinateCoefficient;
    }
// endregion 自动生成的访问器

}
