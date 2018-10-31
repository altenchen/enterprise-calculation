package storm.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;
import storm.dao.DataToRedis;
import storm.dto.FillChargeCar;
import storm.handler.cal.RedisClusterLoaderUseCtfo;
import storm.protocol.CommandType;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.util.ConfigUtils;
import storm.util.DataUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 系统实时数据缓存
 * @author 76304
 *
 */
public class SysRealDataCache {

    private static final Logger LOG = LoggerFactory.getLogger(SysRealDataCache.class);

    private static final DataToRedis DATA_TO_REDIS = new DataToRedis();

    /**
     * 缓冲窗口大小
     */
    private static final int BUFFER_SIZE = 5000000;

    // region LastData

    /**
     * 车辆最近一帧数据 <VID, DATA>
     *
     * 从 CTFO 初始化缓存
     *
     * CarNoticeBolt 会用新到的元组更新 这个缓存
     */
    private static final Cache<String,Map<String,String>> LAST_DATA_CACHE = RedisClusterLoaderUseCtfo.getLastDataCache();

    @Contract(pure = true)
    public static Map<String,Map<String,String>> getLastDataCache(){

        return LAST_DATA_CACHE.asMap();
    }

    /**
     * 车辆最近一帧数据已更新未处理队列
     */
    public static final LinkedBlockingQueue<String> LAST_DATA_QUEUE = Queues.newLinkedBlockingQueue(BUFFER_SIZE);

    /**
     * 车辆最近一帧数据已更新未处理队列防重
     */
    private static final Set<String> LAST_DATA_SET = Sets.newConcurrentHashSet();

    public static void addToLastDataQueue(@NotNull final String vid){
        if (!LAST_DATA_SET.contains(vid)) {
            LAST_DATA_QUEUE.offer(vid);
            LAST_DATA_SET.add(vid);
        }
    }

    /**
     * LAST_DATA_QUEUE 出队之后, 防重标记也要去掉, 这里没有封装好.
     * @param vid
     */
    public static void removeFromLastDataSet(@NotNull final String vid){
        if (LAST_DATA_SET.contains(vid)) {
            LAST_DATA_SET.remove(vid);
        }
    }

    /**
     * 车辆数据（最原始的数据）缓存
     * @param data
     */
    private static void replaceLastDataCache(final ImmutableMap<String, String> data){
        if (MapUtils.isEmpty(data)) {
            return;
        }
        if (!data.containsKey(DataKey.VEHICLE_ID)) {
            return;
        }
        try {
            final Map<String, String> thinData =  Maps.newHashMapWithExpectedSize(data.size());

            // 不缓存无用的数据项，减小缓存大小
            for (final Map.Entry<String, String> entry : data.entrySet()) {
                final String key = entry.getKey();
                final String value = entry.getValue();

                if (StringUtils.isNotBlank(key)
                    && StringUtils.isNotEmpty(value)
                    && !key.startsWith("useful")
                    && !key.startsWith("newest")
                    // 单体蓄电池总数
                    && !"2001".equals(key)
                    // 动力蓄电池包总数
                    && !"2002".equals(key)
                    // 单体蓄电池电压值列表
                    && !"2003".equals(key)
                    // 蓄电池包温度探针总数
                    && !"2101".equals(key)
                    // 单体温度值列表
                    && !"2103".equals(key)
                    //
                    && !"7001".equals(key)
                    // 单体电压原始报文
                    && !"7003".equals(key)
                    && !"7101".equals(key)
                    // 单体文档原始报文
                    && !"7103".equals(key)) {

                    thinData.put(key, value);
                }
            }
            final String vid = thinData.get(DataKey.VEHICLE_ID);
            LAST_DATA_CACHE.put(vid, thinData);
            addToLastDataQueue(vid);
        } catch (Exception e) {
            LOG.warn("更新车辆最后一帧数据异常",  e);
        }
    }

    // endregion LastData

    // region AliveCar

    /**
     * 活跃车辆最近一帧数据<vid, <key, value>>
     *
     * 活跃车辆是指实时数据处理时间距离数据接收时间不超过闲置车辆判定时间范围的车
     * 数据会在这里缓存30天, 最大一千万条记录.
     */
    public static final Cache<String,Map<String,String>> ALIVE_CAR_CACHE = CacheBuilder.newBuilder()
        .expireAfterAccess(30, TimeUnit.DAYS)
        .maximumSize(10000000)
        .build();

    @Contract(pure = true)
    public static Map<String,Map<String,String>> getAliveCarCache(){

        return ALIVE_CAR_CACHE.asMap();
    }

    /**
     * 活跃车辆已更新未处理队列
     */
    public static final LinkedBlockingQueue<String> ALIVE_CAR_QUEUE = Queues.newLinkedBlockingQueue(BUFFER_SIZE);

    /**
     * 活跃车辆已更新未处理队列防重
     */
    private static final Set<String> ALIVE_CAR_SET = Sets.newConcurrentHashSet();


    public static void addToAliveCarQueue(@NotNull final String vid){
        if (!ALIVE_CAR_SET.contains(vid)) {
            ALIVE_CAR_QUEUE.offer(vid);
            ALIVE_CAR_SET.add(vid);
        }
    }

    /**
     * ALIVE_CAR_QUEUE 出队之后, 防重也要去掉, 这里没有封装好.
     * @param vid
     */
    public static void removeFromAliveSet(@NotNull final String vid){
        if (ALIVE_CAR_SET.contains(vid)) {
            ALIVE_CAR_SET.remove(vid);
        }
    }

    /**
     * 最近 idleTimeoutMillisecond 毫秒内的车辆报文加入到 ALIVE_CAR_CACHE 中
     * @param data 数据帧
     * @param currentTimeMillis
     * @param idleTimeoutMillisecond
     * @return 是否活跃车辆
     */
    private static boolean updateAliveCar(
        final ImmutableMap<String, String> data,
        final long currentTimeMillis,
        final long idleTimeoutMillisecond){

        if(MapUtils.isEmpty(data)) {
            return false;
        }
        String vid = null;
        try {

            final String msgType = data.get(DataKey.MESSAGE_TYPE);
            vid = data.get(DataKey.VEHICLE_ID);
            final String time = data.get(DataKey.TIME);

            if(StringUtils.isEmpty(msgType)
                || StringUtils.isEmpty(vid)
                || StringUtils.isEmpty(time)) {
                return false;
            }

            // 当为实时报文且为自动唤醒报文时，忽略
            if(CommandType.SUBMIT_REALTIME.equals(msgType)){
                if(DataUtils.isAutoWake(data)){
                    return false;
                }
            }

            final String utc = data.get(SysDefine.ONLINE_UTC);
            final long utcTime = NumberUtils.toLong(utc, 0);
            final long terminalTime = DateUtils.parseDate(time, new String[]{FormatConstant.DATE_FORMAT}).getTime();
            final long lastTime = Math.max(utcTime, terminalTime);

            if (lastTime > 0) {
                if (currentTimeMillis - lastTime <= idleTimeoutMillisecond) {
                    addToAliveCarQueue(vid);
                    ALIVE_CAR_CACHE.put(vid, data);
                    return true;
                }
            }

        } catch (Exception e) {
            LOG.warn("VID:" + vid + " 判断活跃车辆异常", e);
        }

        return false;
    }

    // endregion AliveCar

    // region CarInfo

    /**
     * 车辆鉴权信息, 目前只使用了其中的"车辆类别"属性, 用于区分充电车.
     *
     * 0 -> XNY.CARINFO
     *
     * FIELD [VIN]
     * VALUE [vid, 终端ID, 车牌号, 使用单位, 存放地点, 汽车厂商, 终端厂商, 终端类型, 车辆类型, 行政区域, 车辆类别, 联系人, 出厂时间, 注册时间, 是否注册]
     */
    private static final Cache<String, String[]> CAR_INFO_CACHE = RedisClusterLoaderUseCtfo.getCarInfoCache();

    /**
     * 车辆鉴权信息缓存 所在 redis 库索引
     */
    private static final int CAR_INFO_DB_INDEX = 0;

    /**
     * 车辆鉴权信息 所在 redis 键
     */
    private static final String CAR_INFO_REDIS_KEY = "XNY.CARINFO";

    /**
     * 固定的车辆信息数据项个数
     */
    private static final int CAR_INFO_ITEM_COUNT = 15;

    /**
     * 车辆类别信息数组下标
     */
    private static final int CAR_TYPE_INFO_INDEX = 10;

    private static final String UNKNOWN = "UNKNOW";

    private static final String[] UNKNOWN_CAR_INFO_ARRAY = new String[]{
        UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN,
        UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN,
        UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN
    };

    /**
     * 最短刷新间隔, 35分钟
     */
    private static final long MIN_REFRESH_TIME_SPAN_MILLISECOND = 1000 * 60 * 35;

    /**
     * 最近一次刷新时间
     */
    private static long carInfoRecentRefreshTime = System.currentTimeMillis();

    private static void resetCarInfoCache(){
        CAR_INFO_CACHE.cleanUp();

        final Map<String, String> map = DATA_TO_REDIS.hashGetAllMapByKeyAndDb(CAR_INFO_REDIS_KEY, CAR_INFO_DB_INDEX);

        if(MapUtils.isEmpty(map)) {
            return;
        }

        for (final Map.Entry<String, String> entry : map.entrySet()) {

            final String vin = entry.getKey();
            final String value = entry.getValue();

            if (StringUtils.isBlank(vin)
                || StringUtils.isEmpty(value)) {
                continue;
            }

            final String[] carInfo = value.split(",");
            if(carInfo.length != CAR_INFO_ITEM_COUNT) {
                continue;
            }

            CAR_INFO_CACHE.put(vin, carInfo);
        }
    }

    /**
     * 获取车辆信息缓存, 并触发自动重置车辆信息.
     * @return
     */
    private static Cache<String, String[]> getCarInfoCache(){
        final long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis - carInfoRecentRefreshTime > MIN_REFRESH_TIME_SPAN_MILLISECOND) {
            carInfoRecentRefreshTime = currentTimeMillis;
            resetCarInfoCache();
        }
        return CAR_INFO_CACHE;
    }

    /**
     * 通过VIN查询车辆信息
     * @param vin
     * @return
     */
    private static String[] getCarInfo(final String vin){
        try {
            return getCarInfoCache().get(vin, () -> UNKNOWN_CAR_INFO_ARRAY);
        } catch (ExecutionException e) {
            LOG.error("获取车辆信息异常", e);
        }
        return UNKNOWN_CAR_INFO_ARRAY;
    }

    // endregion CarInfo

    // region ChargeCar

    /**
     * 补电车类型
     */
    private static final Set<String> CHARGE_CAR_TYPES = Sets.newConcurrentHashSet();

    /**
     * 补电车缓存
     */
    private static final Map<String, FillChargeCar> CHARGE_CAR_CACHE = Maps.newConcurrentMap();

    @Contract(pure = true)
    public static Map<String,FillChargeCar> getChargeCarCache(){
        return CHARGE_CAR_CACHE;
    }

    /**
     * 更新补电车经纬度和在线时间
     * @param data
     */
    private static void updateChargeCar(final ImmutableMap<String, String> data){

        if (MapUtils.isEmpty(data)) {
            return;
        }

        if ( !data.containsKey(DataKey.VEHICLE_ID)
            || !data.containsKey(DataKey.VEHICLE_NUMBER)) {
            return;
        }

        final String vid = data.get(DataKey.VEHICLE_ID);
        final String vin = data.get(DataKey.VEHICLE_NUMBER);

        final String[] carInfo = getCarInfo(vin);
        if(ArrayUtils.isEmpty(carInfo) || carInfo.length != CAR_INFO_ITEM_COUNT) {
            return ;
        }

        // 车辆类别
        final String carTypeId = carInfo[CAR_TYPE_INFO_INDEX];
        if (StringUtils.isBlank(carTypeId) || UNKNOWN.equals(carTypeId)) {
            return;
        }

        if (CHARGE_CAR_TYPES.contains(carTypeId.trim())) {

            final String time = data.get(DataKey.TIME);

            final String longitudeString = data.get(DataKey._2502_LONGITUDE);
            final String latitudeString = data.get(DataKey._2503_LATITUDE);

            if (!StringUtils.isEmpty(time)
                && !StringUtils.isEmpty(latitudeString)
                && !StringUtils.isEmpty(longitudeString)) {

                final double longitude = NumberUtils.toDouble(longitudeString, 0)/1000000.0;;
                final double latitude = NumberUtils.toDouble(latitudeString, 0)/1000000.0;

                final FillChargeCar chargeCar = new FillChargeCar(vid, longitude, latitude, time);
                CHARGE_CAR_CACHE.put(vid, chargeCar);
            }
        }
    }

    // endregion ChargeCar

    // region Static Init

    static {
        try {

            initChargeCarTypeIds();

            initChargeCarCache();

        } catch (Exception e) {
            LOG.error("SysRealDataCache 初始化异常.", e);
        }

    }

    private static void initChargeCarTypeIds() {
        final String chargeCarTypeIdParam = ConfigUtils.getSysParam().getChargeCarTypeId();
        if (!StringUtils.isEmpty(chargeCarTypeIdParam)) {

            final int commaIndex = chargeCarTypeIdParam.indexOf(",");
            final String[] chargeCarTypeIds;
            if (commaIndex > 0) {
                chargeCarTypeIds = chargeCarTypeIdParam.split(",");
            } else {
                chargeCarTypeIds = new String[]{chargeCarTypeIdParam};
            }

            for (int i = 0; i < chargeCarTypeIds.length; i++) {

                final String chargeCarTypeId = chargeCarTypeIds[i];

                if(!StringUtils.isEmpty(chargeCarTypeId)
                    && !CHARGE_CAR_TYPES.contains(chargeCarTypeId)){
                    CHARGE_CAR_TYPES.add(chargeCarTypeId.trim());
                }
            }
        } else {
            CHARGE_CAR_TYPES.add("402894605f511508015f516968890198");
        }
    }

    private static void initChargeCarCache() {
        final Map<String, Map<String, String>> cluster = getLastDataCache();

        for (final Map.Entry<String, Map<String, String>> entry : cluster.entrySet()) {

            ImmutableMap<String, String> data = ImmutableMap.copyOf(entry.getValue());

            updateChargeCar(data);

            updateAliveCar(data, carInfoRecentRefreshTime, ConfigUtils.getSysDefine().getVehicleIdleTimeoutMillisecond());
        }
    }

    /**
     * 触发类构造函数, 连锁触发 RedisClusterLoaderUseCtfo 类初始化
     */
    public static void init(){
    }

    // endregion Static Init

    /**
     * 更新缓存
     * @param data 数据
     * @param currentTimeMillis CarNoticelBolt 收到数据的时间
     */
    public static void updateCache(
        final ImmutableMap<String, String> data,
        final long currentTimeMillis,
        final long idleTimeoutMillisecond){

        // 更新充电车信息
        updateChargeCar(data);

        // 缓存收到最后一帧数据, 不管时间如何
        replaceLastDataCache(data);

        // 缓存收到最后一帧数据, 对报文上传时间和处理时间有时间范围要求.
        updateAliveCar(data, currentTimeMillis, idleTimeoutMillisecond);
    }

}
