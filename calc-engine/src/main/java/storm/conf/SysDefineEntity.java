package storm.conf;

/**
 * 系统运行参数<br/>
 * <p>所有properties中的key包含. _符号的都转换为首字母大写， 具体映射解析 ConfigUtils.keyConvertAttributeName私有方法</p>
 * <p>示例</p>
 * <ul>
 *     <li>stormWorkerNo 对应 storm.worker.no</li>
 *     <li>stormKafkaSpoutNo 对应 storm.kafka.spout.no</li>
 *     <li>kafkaProducerVehicleNoticeTopic 对应 kafka.producer.vehicle_notice.topic</li>
 * </ul>
 * @author xuzhijie
 * @see storm.util.ConfigUtils;
 */
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
    /**
     * 车辆原始报文 topic, 依赖上游前置机, 请保持一致.
     */
    private String kafkaConsumerVehiclePacketDataTopic = null;
    private String kafkaConsumerVehiclePacketDataGroup = null;
    private String kafkaConsumerVehicleRealtimeDataTopic = null;
    private String kafkaConsumerVehicleRealtimeDataGroup = null;
    /**
     * 车辆报警 topic, 下游AlarmService依赖, 请保持一致.
     */
    private String kafkaProducerVehicleAlarmTopic = "SYNC_REALTIME_ALARM";
    /**
     * 车辆报警状态存储 topic, 下游AlarmService依赖, 请保持一致.
     */
    private String kafkaProducerVehicleAlarmStoreTopic = "SYNC_ALARM_STORE";
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
    private int dbCacheFlushTime = 360;

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
    /**
     * 缓存的地理坐标帧数
     */
    private int ctxCacheNo = 18;

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
    private int alarmStartTriggerTimeoutMillisecond = 30000;
    /**
     * 触发平台报警结束需要的连续次数
     */
    private int alarmStopTriggerContinueCount = 3;
    /**
     * 触发平台报警结束需要的持续时长
     */
    private int alarmStopTriggerTimeoutMillisecond = 30000;
    /**
     * 是否启用点火熄火通知 1启用，0关闭
     */
    private int sysIgniteRule = 0;
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
    private int vehicleIdleTimeoutMillisecond = 86400000;
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
    private int noticeCanFaultTriggerTimeoutMillisecond = 30000;
    /**
     * 触发CAN正常需要的连续帧数
     */
    private int noticeCanNormalTriggerContinueCount = 3;
    /**
     * 触发CAN正常需要的持续时长
     */
    private int noticeCanNormalTriggerTimeoutMillisecond = 30000;
    /**
     * 是否启用时间异常通知
     */
    private boolean noticeTimeEnable = true;
    /**
     * 时间数值异常范围
     */
    private int noticeTimeRangeAbsMillisecond = 600000;
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
    private int noticeSocLowBeginTriggerTimeoutMillisecond = 30000;
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
    private int noticeSocLowEndTriggerTimeoutMillisecond = 0;
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
    private int noticeSocHighBeginTriggerTimeoutMillisecond = 30000;
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
    private int noticeSocHighEndTriggerTimeoutMillisecond = 0;
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


    // ############################################################################################
    // 以下配置在sysDefine.properties中没有找到，但是在CarRuleHandler中有使用到，并且是在redis同步过来的数据
    // ############################################################################################
    /**
     * 秒
     */
    private int canJudgeTime = 600;
    /**
     * 连续多少帧没有can状态，算为无can状态车辆
     */
    private int canNovalueContinueNo = 5;
    /**
     * 里程跳变数，单位是km, 2表示2公里
     */
    private int mileHopNum = 2;

    public int getStormWorkerNo() {
        return stormWorkerNo;
    }

    public void setStormWorkerNo(int stormWorkerNo) {
        this.stormWorkerNo = stormWorkerNo;
    }

    public int getStormKafkaSpoutNo() {
        return stormKafkaSpoutNo;
    }

    public void setStormKafkaSpoutNo(int stormKafkaSpoutNo) {
        this.stormKafkaSpoutNo = stormKafkaSpoutNo;
    }

    public int getStormKafkaBoltNo() {
        return stormKafkaBoltNo;
    }

    public void setStormKafkaBoltNo(int stormKafkaBoltNo) {
        this.stormKafkaBoltNo = stormKafkaBoltNo;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public String getKafkaZookeeperServers() {
        return kafkaZookeeperServers;
    }

    public void setKafkaZookeeperServers(String kafkaZookeeperServers) {
        this.kafkaZookeeperServers = kafkaZookeeperServers;
    }

    public int getKafkaZookeeperPort() {
        return kafkaZookeeperPort;
    }

    public void setKafkaZookeeperPort(int kafkaZookeeperPort) {
        this.kafkaZookeeperPort = kafkaZookeeperPort;
    }

    public String getKafkaZookeeperPath() {
        return kafkaZookeeperPath;
    }

    public void setKafkaZookeeperPath(String kafkaZookeeperPath) {
        this.kafkaZookeeperPath = kafkaZookeeperPath;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaConsumerVehiclePacketDataTopic() {
        return kafkaConsumerVehiclePacketDataTopic;
    }

    public void setKafkaConsumerVehiclePacketDataTopic(String kafkaConsumerVehiclePacketDataTopic) {
        this.kafkaConsumerVehiclePacketDataTopic = kafkaConsumerVehiclePacketDataTopic;
    }

    public String getKafkaConsumerVehiclePacketDataGroup() {
        return kafkaConsumerVehiclePacketDataGroup;
    }

    public void setKafkaConsumerVehiclePacketDataGroup(String kafkaConsumerVehiclePacketDataGroup) {
        this.kafkaConsumerVehiclePacketDataGroup = kafkaConsumerVehiclePacketDataGroup;
    }

    public String getKafkaConsumerVehicleRealtimeDataTopic() {
        return kafkaConsumerVehicleRealtimeDataTopic;
    }

    public void setKafkaConsumerVehicleRealtimeDataTopic(String kafkaConsumerVehicleRealtimeDataTopic) {
        this.kafkaConsumerVehicleRealtimeDataTopic = kafkaConsumerVehicleRealtimeDataTopic;
    }

    public String getKafkaConsumerVehicleRealtimeDataGroup() {
        return kafkaConsumerVehicleRealtimeDataGroup;
    }

    public void setKafkaConsumerVehicleRealtimeDataGroup(String kafkaConsumerVehicleRealtimeDataGroup) {
        this.kafkaConsumerVehicleRealtimeDataGroup = kafkaConsumerVehicleRealtimeDataGroup;
    }

    public String getKafkaProducerVehicleAlarmTopic() {
        return kafkaProducerVehicleAlarmTopic;
    }

    public void setKafkaProducerVehicleAlarmTopic(String kafkaProducerVehicleAlarmTopic) {
        this.kafkaProducerVehicleAlarmTopic = kafkaProducerVehicleAlarmTopic;
    }

    public String getKafkaProducerVehicleAlarmStoreTopic() {
        return kafkaProducerVehicleAlarmStoreTopic;
    }

    public void setKafkaProducerVehicleAlarmStoreTopic(String kafkaProducerVehicleAlarmStoreTopic) {
        this.kafkaProducerVehicleAlarmStoreTopic = kafkaProducerVehicleAlarmStoreTopic;
    }

    public String getKafkaProducerVehicleFenceAlarmTopic() {
        return kafkaProducerVehicleFenceAlarmTopic;
    }

    public void setKafkaProducerVehicleFenceAlarmTopic(String kafkaProducerVehicleFenceAlarmTopic) {
        this.kafkaProducerVehicleFenceAlarmTopic = kafkaProducerVehicleFenceAlarmTopic;
    }

    public String getKafkaProducerVehicleNoticeTopic() {
        return kafkaProducerVehicleNoticeTopic;
    }

    public void setKafkaProducerVehicleNoticeTopic(String kafkaProducerVehicleNoticeTopic) {
        this.kafkaProducerVehicleNoticeTopic = kafkaProducerVehicleNoticeTopic;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public String getRedisPass() {
        return redisPass;
    }

    public void setRedisPass(String redisPass) {
        this.redisPass = redisPass;
    }

    public int getRedisMaxActive() {
        return redisMaxActive;
    }

    public void setRedisMaxActive(int redisMaxActive) {
        this.redisMaxActive = redisMaxActive;
    }

    public int getRedisMaxIdle() {
        return redisMaxIdle;
    }

    public void setRedisMaxIdle(int redisMaxIdle) {
        this.redisMaxIdle = redisMaxIdle;
    }

    public int getRedisMaxWait() {
        return redisMaxWait;
    }

    public void setRedisMaxWait(int redisMaxWait) {
        this.redisMaxWait = redisMaxWait;
    }

    public int getRedisTimeOut() {
        return redisTimeOut;
    }

    public void setRedisTimeOut(int redisTimeOut) {
        this.redisTimeOut = redisTimeOut;
    }

    public int getRedisTimeInterval() {
        return redisTimeInterval;
    }

    public void setRedisTimeInterval(int redisTimeInterval) {
        this.redisTimeInterval = redisTimeInterval;
    }

    public int getRedisListenInterval() {
        return redisListenInterval;
    }

    public void setRedisListenInterval(int redisListenInterval) {
        this.redisListenInterval = redisListenInterval;
    }

    public int getRedisTotalInterval() {
        return redisTotalInterval;
    }

    public void setRedisTotalInterval(int redisTotalInterval) {
        this.redisTotalInterval = redisTotalInterval;
    }

    public String getCtfoCacheHost() {
        return ctfoCacheHost;
    }

    public void setCtfoCacheHost(String ctfoCacheHost) {
        this.ctfoCacheHost = ctfoCacheHost;
    }

    public int getCtfoCachePort() {
        return ctfoCachePort;
    }

    public void setCtfoCachePort(int ctfoCachePort) {
        this.ctfoCachePort = ctfoCachePort;
    }

    public String getCtfoCacheDB() {
        return ctfoCacheDB;
    }

    public void setCtfoCacheDB(String ctfoCacheDB) {
        this.ctfoCacheDB = ctfoCacheDB;
    }

    public String getCtfoCacheTable() {
        return ctfoCacheTable;
    }

    public void setCtfoCacheTable(String ctfoCacheTable) {
        this.ctfoCacheTable = ctfoCacheTable;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getJdbcUsername() {
        return jdbcUsername;
    }

    public void setJdbcUsername(String jdbcUsername) {
        this.jdbcUsername = jdbcUsername;
    }

    public String getJdbcPassword() {
        return jdbcPassword;
    }

    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    public int getDbCacheFlushTime() {
        return dbCacheFlushTime;
    }

    public void setDbCacheFlushTime(int dbCacheFlushTime) {
        this.dbCacheFlushTime = dbCacheFlushTime;
    }

    public int getRedisOfflineTime() {
        return redisOfflineTime;
    }

    public void setRedisOfflineTime(int redisOfflineTime) {
        this.redisOfflineTime = redisOfflineTime;
    }

    public int getRedisOfflineStopTime() {
        return redisOfflineStopTime;
    }

    public void setRedisOfflineStopTime(int redisOfflineStopTime) {
        this.redisOfflineStopTime = redisOfflineStopTime;
    }

    public int getRedisMonitorTime() {
        return redisMonitorTime;
    }

    public void setRedisMonitorTime(int redisMonitorTime) {
        this.redisMonitorTime = redisMonitorTime;
    }

    public int getStatThreadNo() {
        return statThreadNo;
    }

    public void setStatThreadNo(int statThreadNo) {
        this.statThreadNo = statThreadNo;
    }

    public int getCtxCacheNo() {
        return ctxCacheNo;
    }

    public void setCtxCacheNo(int ctxCacheNo) {
        this.ctxCacheNo = ctxCacheNo;
    }

    public int getRedisClusterDataSyn() {
        return redisClusterDataSyn;
    }

    public void setRedisClusterDataSyn(int redisClusterDataSyn) {
        this.redisClusterDataSyn = redisClusterDataSyn;
    }

    public int getRedisOfflineCheckTime() {
        return redisOfflineCheckTime;
    }

    public void setRedisOfflineCheckTime(int redisOfflineCheckTime) {
        this.redisOfflineCheckTime = redisOfflineCheckTime;
    }

    public int getAlarmStartTriggerContinueCount() {
        return alarmStartTriggerContinueCount;
    }

    public void setAlarmStartTriggerContinueCount(int alarmStartTriggerContinueCount) {
        this.alarmStartTriggerContinueCount = alarmStartTriggerContinueCount;
    }

    public int getAlarmStartTriggerTimeoutMillisecond() {
        return alarmStartTriggerTimeoutMillisecond;
    }

    public void setAlarmStartTriggerTimeoutMillisecond(int alarmStartTriggerTimeoutMillisecond) {
        this.alarmStartTriggerTimeoutMillisecond = alarmStartTriggerTimeoutMillisecond;
    }

    public int getAlarmStopTriggerContinueCount() {
        return alarmStopTriggerContinueCount;
    }

    public void setAlarmStopTriggerContinueCount(int alarmStopTriggerContinueCount) {
        this.alarmStopTriggerContinueCount = alarmStopTriggerContinueCount;
    }

    public int getAlarmStopTriggerTimeoutMillisecond() {
        return alarmStopTriggerTimeoutMillisecond;
    }

    public void setAlarmStopTriggerTimeoutMillisecond(int alarmStopTriggerTimeoutMillisecond) {
        this.alarmStopTriggerTimeoutMillisecond = alarmStopTriggerTimeoutMillisecond;
    }

    public int getSysIgniteRule() {
        return sysIgniteRule;
    }

    public void setSysIgniteRule(int sysIgniteRule) {
        this.sysIgniteRule = sysIgniteRule;
    }

    public int getSysAbnormalRule() {
        return sysAbnormalRule;
    }

    public void setSysAbnormalRule(int sysAbnormalRule) {
        this.sysAbnormalRule = sysAbnormalRule;
    }

    public int getSysFlyRule() {
        return sysFlyRule;
    }

    public void setSysFlyRule(int sysFlyRule) {
        this.sysFlyRule = sysFlyRule;
    }

    public int getSysOnOffRule() {
        return sysOnOffRule;
    }

    public void setSysOnOffRule(int sysOnOffRule) {
        this.sysOnOffRule = sysOnOffRule;
    }

    public int getSysMilehopRule() {
        return sysMilehopRule;
    }

    public void setSysMilehopRule(int sysMilehopRule) {
        this.sysMilehopRule = sysMilehopRule;
    }

    public int getSysCarLockStatusRule() {
        return sysCarLockStatusRule;
    }

    public void setSysCarLockStatusRule(int sysCarLockStatusRule) {
        this.sysCarLockStatusRule = sysCarLockStatusRule;
    }

    public int getVehicleIdleTimeoutMillisecond() {
        return vehicleIdleTimeoutMillisecond;
    }

    public void setVehicleIdleTimeoutMillisecond(int vehicleIdleTimeoutMillisecond) {
        this.vehicleIdleTimeoutMillisecond = vehicleIdleTimeoutMillisecond;
    }

    public int getSysCanRule() {
        return sysCanRule;
    }

    public void setSysCanRule(int sysCanRule) {
        this.sysCanRule = sysCanRule;
    }

    public int getNoticeCanFaultTriggerContinueCount() {
        return noticeCanFaultTriggerContinueCount;
    }

    public void setNoticeCanFaultTriggerContinueCount(int noticeCanFaultTriggerContinueCount) {
        this.noticeCanFaultTriggerContinueCount = noticeCanFaultTriggerContinueCount;
    }

    public int getNoticeCanFaultTriggerTimeoutMillisecond() {
        return noticeCanFaultTriggerTimeoutMillisecond;
    }

    public void setNoticeCanFaultTriggerTimeoutMillisecond(int noticeCanFaultTriggerTimeoutMillisecond) {
        this.noticeCanFaultTriggerTimeoutMillisecond = noticeCanFaultTriggerTimeoutMillisecond;
    }

    public int getNoticeCanNormalTriggerContinueCount() {
        return noticeCanNormalTriggerContinueCount;
    }

    public void setNoticeCanNormalTriggerContinueCount(int noticeCanNormalTriggerContinueCount) {
        this.noticeCanNormalTriggerContinueCount = noticeCanNormalTriggerContinueCount;
    }

    public int getNoticeCanNormalTriggerTimeoutMillisecond() {
        return noticeCanNormalTriggerTimeoutMillisecond;
    }

    public void setNoticeCanNormalTriggerTimeoutMillisecond(int noticeCanNormalTriggerTimeoutMillisecond) {
        this.noticeCanNormalTriggerTimeoutMillisecond = noticeCanNormalTriggerTimeoutMillisecond;
    }

    public boolean isNoticeTimeEnable() {
        return noticeTimeEnable;
    }

    public void setNoticeTimeEnable(boolean noticeTimeEnable) {
        this.noticeTimeEnable = noticeTimeEnable;
    }

    public int getNoticeTimeRangeAbsMillisecond() {
        return noticeTimeRangeAbsMillisecond;
    }

    public void setNoticeTimeRangeAbsMillisecond(int noticeTimeRangeAbsMillisecond) {
        this.noticeTimeRangeAbsMillisecond = noticeTimeRangeAbsMillisecond;
    }

    public boolean isNoticeSocLowEnable() {
        return noticeSocLowEnable;
    }

    public void setNoticeSocLowEnable(boolean noticeSocLowEnable) {
        this.noticeSocLowEnable = noticeSocLowEnable;
    }

    public int getNoticeSocLowBeginTriggerThreshold() {
        return noticeSocLowBeginTriggerThreshold;
    }

    public void setNoticeSocLowBeginTriggerThreshold(int noticeSocLowBeginTriggerThreshold) {
        this.noticeSocLowBeginTriggerThreshold = noticeSocLowBeginTriggerThreshold;
    }

    public int getNoticeSocLowBeginTriggerContinueCount() {
        return noticeSocLowBeginTriggerContinueCount;
    }

    public void setNoticeSocLowBeginTriggerContinueCount(int noticeSocLowBeginTriggerContinueCount) {
        this.noticeSocLowBeginTriggerContinueCount = noticeSocLowBeginTriggerContinueCount;
    }

    public int getNoticeSocLowBeginTriggerTimeoutMillisecond() {
        return noticeSocLowBeginTriggerTimeoutMillisecond;
    }

    public void setNoticeSocLowBeginTriggerTimeoutMillisecond(int noticeSocLowBeginTriggerTimeoutMillisecond) {
        this.noticeSocLowBeginTriggerTimeoutMillisecond = noticeSocLowBeginTriggerTimeoutMillisecond;
    }

    public int getNoticeSocLowEndTriggerThreshold() {
        return noticeSocLowEndTriggerThreshold;
    }

    public void setNoticeSocLowEndTriggerThreshold(int noticeSocLowEndTriggerThreshold) {
        this.noticeSocLowEndTriggerThreshold = noticeSocLowEndTriggerThreshold;
    }

    public int getNoticeSocLowEndTriggerContinueCount() {
        return noticeSocLowEndTriggerContinueCount;
    }

    public void setNoticeSocLowEndTriggerContinueCount(int noticeSocLowEndTriggerContinueCount) {
        this.noticeSocLowEndTriggerContinueCount = noticeSocLowEndTriggerContinueCount;
    }

    public int getNoticeSocLowEndTriggerTimeoutMillisecond() {
        return noticeSocLowEndTriggerTimeoutMillisecond;
    }

    public void setNoticeSocLowEndTriggerTimeoutMillisecond(int noticeSocLowEndTriggerTimeoutMillisecond) {
        this.noticeSocLowEndTriggerTimeoutMillisecond = noticeSocLowEndTriggerTimeoutMillisecond;
    }

    public boolean isNoticeSocHighEnable() {
        return noticeSocHighEnable;
    }

    public void setNoticeSocHighEnable(boolean noticeSocHighEnable) {
        this.noticeSocHighEnable = noticeSocHighEnable;
    }

    public int getNoticeSocHighBeginTriggerThreshold() {
        return noticeSocHighBeginTriggerThreshold;
    }

    public void setNoticeSocHighBeginTriggerThreshold(int noticeSocHighBeginTriggerThreshold) {
        this.noticeSocHighBeginTriggerThreshold = noticeSocHighBeginTriggerThreshold;
    }

    public int getNoticeSocHighBeginTriggerContinueCount() {
        return noticeSocHighBeginTriggerContinueCount;
    }

    public void setNoticeSocHighBeginTriggerContinueCount(int noticeSocHighBeginTriggerContinueCount) {
        this.noticeSocHighBeginTriggerContinueCount = noticeSocHighBeginTriggerContinueCount;
    }

    public int getNoticeSocHighBeginTriggerTimeoutMillisecond() {
        return noticeSocHighBeginTriggerTimeoutMillisecond;
    }

    public void setNoticeSocHighBeginTriggerTimeoutMillisecond(int noticeSocHighBeginTriggerTimeoutMillisecond) {
        this.noticeSocHighBeginTriggerTimeoutMillisecond = noticeSocHighBeginTriggerTimeoutMillisecond;
    }

    public int getNoticeSocHighEndTriggerThreshold() {
        return noticeSocHighEndTriggerThreshold;
    }

    public void setNoticeSocHighEndTriggerThreshold(int noticeSocHighEndTriggerThreshold) {
        this.noticeSocHighEndTriggerThreshold = noticeSocHighEndTriggerThreshold;
    }

    public int getNoticeSocHighEndTriggerContinueCount() {
        return noticeSocHighEndTriggerContinueCount;
    }

    public void setNoticeSocHighEndTriggerContinueCount(int noticeSocHighEndTriggerContinueCount) {
        this.noticeSocHighEndTriggerContinueCount = noticeSocHighEndTriggerContinueCount;
    }

    public int getNoticeSocHighEndTriggerTimeoutMillisecond() {
        return noticeSocHighEndTriggerTimeoutMillisecond;
    }

    public void setNoticeSocHighEndTriggerTimeoutMillisecond(int noticeSocHighEndTriggerTimeoutMillisecond) {
        this.noticeSocHighEndTriggerTimeoutMillisecond = noticeSocHighEndTriggerTimeoutMillisecond;
    }

    public int getCanNovalueContinueNo() {
        return canNovalueContinueNo;
    }

    public void setCanNovalueContinueNo(int canNovalueContinueNo) {
        this.canNovalueContinueNo = canNovalueContinueNo;
    }

    public int getSysGpsRule() {
        return sysGpsRule;
    }

    public void setSysGpsRule(int sysGpsRule) {
        this.sysGpsRule = sysGpsRule;
    }

    public int getGpsNovalueContinueNo() {
        return gpsNovalueContinueNo;
    }

    public void setGpsNovalueContinueNo(int gpsNovalueContinueNo) {
        this.gpsNovalueContinueNo = gpsNovalueContinueNo;
    }

    public int getGpsHasvalueContinueNo() {
        return gpsHasvalueContinueNo;
    }

    public void setGpsHasvalueContinueNo(int gpsHasvalueContinueNo) {
        this.gpsHasvalueContinueNo = gpsHasvalueContinueNo;
    }

    public int getGpsJudgeTime() {
        return gpsJudgeTime;
    }

    public void setGpsJudgeTime(int gpsJudgeTime) {
        this.gpsJudgeTime = gpsJudgeTime;
    }

    public String getRuleOverride() {
        return ruleOverride;
    }

    public void setRuleOverride(String ruleOverride) {
        this.ruleOverride = ruleOverride;
    }

    public int getCanJudgeTime() {
        return canJudgeTime;
    }

    public void setCanJudgeTime(int canJudgeTime) {
        this.canJudgeTime = canJudgeTime;
    }

    public int getMileHopNum() {
        return mileHopNum;
    }

    public void setMileHopNum(int mileHopNum) {
        this.mileHopNum = mileHopNum;
    }

    public int getStormWorkerHeapMemoryMb() {
        return stormWorkerHeapMemoryMb;
    }
}
