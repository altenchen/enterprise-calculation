package storm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleCache;
import storm.constant.FormatConstant;
import storm.extension.ObjectExtension;
import storm.handler.cusmade.CarNoCanJudge;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.util.ConfigUtils;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;

/**
 * @author: xzp
 * @date: 2018-07-09
 * @description:
 */
@Disabled
@DisplayName("无CAN通知测试")
final class CarNoCanJudgeTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CarNoCanJudgeTest.class);
    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();
    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    private static JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static final JsonUtils GSON_UTILS = JsonUtils.getInstance();

    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    private static final String TEST_VID = "TV-" + UUID.randomUUID();

    private static final int REDIS_DB_INDEX = VehicleCache.REDIS_DB_INDEX;
    private static final String REDIS_KEY = VehicleCache.buildRedisKey(TEST_VID);

    private static final String usefulTotalMileage = String.valueOf(
        Math.abs(
            new Random().nextInt(10000)) + 10000);

    private CarNoCanJudgeTest() {
    }

    @SuppressWarnings("unused")
    @BeforeAll
    private static void beforeAll() {
        // 所有测试之前
        JEDIS_POOL_UTILS.useResource(jedis -> {

            jedis.select(REDIS_DB_INDEX);

            final ImmutableMap<String, String> totalMileage = new ImmutableMap.Builder<String, String>()
                .put(VehicleCache.VALUE_TIME_KEY, DateFormatUtils.format(System.currentTimeMillis(), FormatConstant.DATE_FORMAT))
                .put(VehicleCache.VALUE_DATA_KEY, usefulTotalMileage)
                .build();

            final String json = GSON_UTILS.toJson(totalMileage);
            jedis.hset(
                REDIS_KEY,
                VehicleCache.TOTAL_MILEAGE_FIELD,
                json);
        });

        ConfigUtils.getSysDefine().setNoticeCanFaultTriggerContinueCount(3);
        ConfigUtils.getSysDefine().setNoticeCanFaultTriggerTimeoutMillisecond(0);
        ConfigUtils.getSysDefine().setNoticeCanNormalTriggerContinueCount(3);
        ConfigUtils.getSysDefine().setNoticeCanNormalTriggerTimeoutMillisecond(0);
    }

    @SuppressWarnings("unused")
    @BeforeEach
    private void beforeEach() {
        // 每个测试之前
    }

    @DisplayName("吉利故障测试")
    @Test
    void carNoCanDecideJiliFault() {

        final long currentTimeMillis = System.currentTimeMillis();

        final CarNoCanJudge carNoCanJudge = new CarNoCanJudge();

        final Map<String, String> data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey._2502_LONGITUDE, "100");
        data.put(DataKey._2503_LATITUDE, "100");

        // region 连续帧未达到阈值不发出通知

        data.put(DataKey._7615_STATE_OF_CHARGE, "");
        appendTime(data, currentTimeMillis, 101000);
        final String processFrame1 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame1), "第1帧不该出现故障通知");
        appendTime(data, currentTimeMillis, 102000);
        final String processFrame2 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame2), "第2帧不该出现故障通知");
        appendTime(data, currentTimeMillis, 103000);
        data.put(DataKey._7615_STATE_OF_CHARGE, "80");
        final String processFrame3 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame3), "第3帧不该出现故障通知");

        // endregion 连续帧未达到阈值不发出通知

        // region 连续帧取首帧的数据作为通知数据, 残缺的数据从缓存中取最后有效值.

        data.put(DataKey._7615_STATE_OF_CHARGE, "");
        String formatTime4 = appendTime(data, currentTimeMillis, 104000);
        final String processFrame4 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame4), "第4帧不该出现故障通知");
        appendTime(data, currentTimeMillis, 105000);
        final String processFrame5 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame5), "第5帧不该出现故障通知");
        appendTime(data, currentTimeMillis, 106000);
        final String processFrame6 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isNotBlank(processFrame6), "第6帧应该出现故障通知");
        LOG.info("notice : {}", processFrame6);

        // 故障告警状态检查
        ImmutableMap<String, String> processFrame6Map = convertJson(processFrame6);
        Assertions.assertEquals(
            "1",
            ObjectUtils.toString(processFrame6Map.get("status")),
            "没有从缓存获取到正确的时间");
        // 取连续故障帧的首帧值
        Assertions.assertEquals(formatTime4, processFrame6Map.get("stime"), "没有从缓存获取到正确的时间");
        // 从缓存获取累计里程
        Assertions.assertEquals(usefulTotalMileage, processFrame6Map.get("smileage"), "没有从缓存获取到正确的累计里程");
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            final String json = jedis.hget(REDIS_KEY, NoticeType.NO_CAN_VEH);
            final Map<String, Object> notice = GSON_UTILS.fromJson(
                json,
                new TypeToken<TreeMap<String, Object>>() {
                }.getType());
            Assertions.assertEquals(processFrame6Map, notice, "缓存中的故障通知与内存中的不一致");
        });

        // endregion 连续帧取首帧的数据作为通知数据, 残缺的数据从缓存中取最后有效值.

        // region 持续故障帧不再发出通知

        appendTime(data, currentTimeMillis, 107000);
        final String processFrame7 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame7), "第7帧不该出现故障通知");
        appendTime(data, currentTimeMillis, 108000);
        final String processFrame8 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame8), "第8帧不该出现故障通知");
        appendTime(data, currentTimeMillis, 109000);
        final String processFrame9 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame9), "第9帧不该出现故障通知");

        // endregion 持续故障帧不再发出通知
    }

    private String appendTime(Map<String, String> data, long currentTimeMillis, long time) {
        String timeString = DateFormatUtils.format(currentTimeMillis + time, FormatConstant.DATE_FORMAT);
        data.put(DataKey.TIME, timeString);
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, timeString);
        return timeString;
    }

    private ImmutableMap<String, String> convertJson(String json) {
        return ImmutableMap.copyOf(ObjectExtension.defaultIfNull(
            JSON_UTILS.fromJson(
                json,
                TREE_MAP_STRING_STRING_TYPE,
                e -> {
                    return null;
                }),
            Maps::newTreeMap));
    }

    @DisplayName("吉利正常测试")
    @Test
    void carNoCanDecideJiliNormal() {

        final long currentTimeMillis = System.currentTimeMillis();

        final CarNoCanJudge carNoCanJudge = new CarNoCanJudge();

        final Map<String, String> data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey._2502_LONGITUDE, "100");
        data.put(DataKey._2503_LATITUDE, "100");

        // region 确保CAN故障

        data.put(DataKey._7615_STATE_OF_CHARGE, "");
        appendTime(data, currentTimeMillis, 201000);
        carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        appendTime(data, currentTimeMillis, 202000);
        carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        appendTime(data, currentTimeMillis, 203000);
        carNoCanJudge.processFrame(ImmutableMap.copyOf(data));

        // endregion 确保CAN故障


        // region 连续帧未达到阈值不发出通知

        data.put(DataKey._7615_STATE_OF_CHARGE, "80");
        appendTime(data, currentTimeMillis, 211000);
        final String processFrame1 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame1), "第1帧不该出现恢复通知");
        appendTime(data, currentTimeMillis, 212000);
        final String processFrame2 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame2), "第2帧不该出现恢复通知");
        appendTime(data, currentTimeMillis, 213000);
        data.put(DataKey._7615_STATE_OF_CHARGE, "");
        final String processFrame3 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame3), "第3帧不该出现恢复通知");

        // endregion 连续帧未达到阈值不发出通知

        // region 连续帧取首帧的数据作为通知数据, 残缺的数据从缓存中取最后有效值.

        data.put(DataKey._7615_STATE_OF_CHARGE, "80");
        String formatTime4 = appendTime(data, currentTimeMillis, 104000);
        final String processFrame4 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame4), "第4帧不该出现恢复通知");
        appendTime(data, currentTimeMillis, 105000);
        final String processFrame5 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame5), "第5帧不该出现恢复通知");
        appendTime(data, currentTimeMillis, 106000);
        final String processFrame6 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isNotBlank(processFrame6), "第6帧应该出现恢复通知");
        LOG.info("notice : {}", processFrame6);

        // 故障告警状态检查
        ImmutableMap<String, String> processFrame6Map = convertJson(processFrame6);
        Assertions.assertEquals(
            "3",
            ObjectUtils.toString(processFrame6Map.get("status")),
            "没有从缓存获取到正确的时间");
        // 取连续故障帧的首帧值
        Assertions.assertEquals(formatTime4, processFrame6Map.get("etime"), "没有从缓存获取到正确的时间");
        // 从缓存获取累计里程
        Assertions.assertEquals(usefulTotalMileage, processFrame6Map.get("emileage"), "没有从缓存获取到正确的累计里程");
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            final String json = jedis.hget(REDIS_KEY, NoticeType.NO_CAN_VEH);
            Assertions.assertNull(json, "缓存中的不应存在已恢复通知");
        });

        // endregion 连续帧取首帧的数据作为通知数据, 残缺的数据从缓存中取最后有效值.

        // region 持续故障帧不再发出通知

        appendTime(data, currentTimeMillis, 107000);
        final String processFrame7 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame7), "第7帧不该出现恢复通知");
        appendTime(data, currentTimeMillis, 108000);
        final String processFrame8 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame8), "第8帧不该出现恢复通知");
        appendTime(data, currentTimeMillis, 109000);
        final String processFrame9 = carNoCanJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(processFrame9), "第9帧不该出现恢复通知");

        // endregion 持续故障帧不再发出通知
    }

    @SuppressWarnings("unused")
    @AfterEach
    private void afterEach() {
        // 每个测试之后
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            jedis.hdel(REDIS_KEY, NoticeType.NO_CAN_VEH);
        });
    }

    @SuppressWarnings("unused")
    @AfterAll
    private static void afterAll() {
        // 所有测试之后
        VEHICLE_CACHE.delFields(TEST_VID);
    }
}
