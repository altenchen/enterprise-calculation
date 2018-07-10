package storm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleCache;
import storm.constant.FormatConstant;
import storm.dto.fault.AlarmMessage;
import storm.handler.cusmade.CarNoCanDecideJili;
import storm.handler.cusmade.CarNoCanJudge;
import storm.system.AlarmMessageType;
import storm.system.DataKey;
import storm.util.GsonUtils;
import storm.util.JedisPoolUtils;

import java.util.*;

/**
 * @author: xzp
 * @date: 2018-07-09
 * @description:
 */
@Disabled
@DisplayName("无CAN通知测试")
final class CarNoCanJudgeTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CarNoCanJudgeTest.class);

    private static JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static final GsonUtils GSON_UTILS = GsonUtils.getInstance();

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

        CarNoCanJudge.setFaultTriggerContinueCount(3);
        CarNoCanJudge.setFaultTriggerTimeoutMillisecond(0);
        CarNoCanJudge.setNormalTriggerContinueCount(3);
        CarNoCanJudge.setNormalTriggerTimeoutMillisecond(0);
    }

    @SuppressWarnings("unused")
    @BeforeEach
    private void beforeEach() {
        // 每个测试之前
    }

    @DisplayName("吉利故障测试")
    @Test
    void carNoCanDecideJiliFault() {

        CarNoCanJudge.setCarNoCanDecide(new CarNoCanDecideJili());

        final long currentTimeMillis = System.currentTimeMillis();

        final CarNoCanJudge carNoCanJudge = new CarNoCanJudge();

        final Map<String, String> data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey._2502_LONGITUDE, "100");
        data.put(DataKey._2503_LATITUDE, "100");

        // region 连续帧未达到阈值不发出通知

        data.put(DataKey._7615_STATE_OF_CHARGE, "");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 101000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame1 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame1), "第1帧不该出现故障通知");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 102000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame2 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame2), "第2帧不该出现故障通知");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 103000, FormatConstant.DATE_FORMAT));
        data.put(DataKey._7615_STATE_OF_CHARGE, "80");
        final Map<String, Object> processFrame3 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame3), "第3帧不该出现故障通知");

        // endregion 连续帧未达到阈值不发出通知

        // region 连续帧取首帧的数据作为通知数据, 残缺的数据从缓存中取最后有效值.

        data.put(DataKey._7615_STATE_OF_CHARGE, "");
        final String formatTime4 = DateFormatUtils.format(currentTimeMillis + 104000, FormatConstant.DATE_FORMAT);
        data.put(DataKey.TIME, formatTime4);
        final Map<String, Object> processFrame4 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame4), "第4帧不该出现故障通知");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 105000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame5 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame5), "第5帧不该出现故障通知");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 106000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame6 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isNotEmpty(processFrame6), "第6帧应该出现故障通知");
        // 故障告警状态检查
        Assertions.assertEquals(
            "1",
            ObjectUtils.toString(processFrame6.get("status")),
            "没有从缓存获取到正确的时间");
        // 取连续故障帧的首帧值
        Assertions.assertEquals(formatTime4, processFrame6.get("stime"), "没有从缓存获取到正确的时间");
        // 从缓存获取累计里程
        Assertions.assertEquals(usefulTotalMileage, processFrame6.get("smileage"), "没有从缓存获取到正确的累计里程");
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            final String json = jedis.hget(REDIS_KEY, AlarmMessageType.NO_CAN_VEH);
            final Map<String, Object> notice = GSON_UTILS.fromJson(
                json,
                new TypeToken<TreeMap<String, Object>>() {
                }.getType());
            Assertions.assertEquals(processFrame6, notice, "缓存中的故障通知与内存中的不一致");
        });

        // endregion 连续帧取首帧的数据作为通知数据, 残缺的数据从缓存中取最后有效值.

        // region 持续故障帧不再发出通知

        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 107000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame7 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame7), "第7帧不该出现故障通知");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 108000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame8 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame8), "第8帧不该出现故障通知");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 109000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame9 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame9), "第9帧不该出现故障通知");

        // endregion 持续故障帧不再发出通知
    }

    @DisplayName("吉利正常测试")
    @Test
    void carNoCanDecideJiliNormal() {

        CarNoCanJudge.setCarNoCanDecide(new CarNoCanDecideJili());

        final long currentTimeMillis = System.currentTimeMillis();

        final CarNoCanJudge carNoCanJudge = new CarNoCanJudge();

        final Map<String, String> data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey._2502_LONGITUDE, "100");
        data.put(DataKey._2503_LATITUDE, "100");

        // region 确保CAN故障

        data.put(DataKey._7615_STATE_OF_CHARGE, "");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 201000, FormatConstant.DATE_FORMAT));
        carNoCanJudge.processFrame(data);
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 202000, FormatConstant.DATE_FORMAT));
        carNoCanJudge.processFrame(data);
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 203000, FormatConstant.DATE_FORMAT));
        carNoCanJudge.processFrame(data);

        // endregion 确保CAN故障


        // region 连续帧未达到阈值不发出通知

        data.put(DataKey._7615_STATE_OF_CHARGE, "80");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 211000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame1 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame1), "第1帧不该出现恢复通知");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 212000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame2 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame2), "第2帧不该出现恢复通知");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 213000, FormatConstant.DATE_FORMAT));
        data.put(DataKey._7615_STATE_OF_CHARGE, "");
        final Map<String, Object> processFrame3 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame3), "第3帧不该出现恢复通知");

        // endregion 连续帧未达到阈值不发出通知

        // region 连续帧取首帧的数据作为通知数据, 残缺的数据从缓存中取最后有效值.

        data.put(DataKey._7615_STATE_OF_CHARGE, "80");
        final String formatTime4 = DateFormatUtils.format(currentTimeMillis + 104000, FormatConstant.DATE_FORMAT);
        data.put(DataKey.TIME, formatTime4);
        final Map<String, Object> processFrame4 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame4), "第4帧不该出现恢复通知");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 105000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame5 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame5), "第5帧不该出现恢复通知");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 106000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame6 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isNotEmpty(processFrame6), "第6帧应该出现恢复通知");
        // 故障告警状态检查
        Assertions.assertEquals(
            "3",
            ObjectUtils.toString(processFrame6.get("status")),
            "没有从缓存获取到正确的时间");
        // 取连续故障帧的首帧值
        Assertions.assertEquals(formatTime4, processFrame6.get("etime"), "没有从缓存获取到正确的时间");
        // 从缓存获取累计里程
        Assertions.assertEquals(usefulTotalMileage, processFrame6.get("emileage"), "没有从缓存获取到正确的累计里程");
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            final String json = jedis.hget(REDIS_KEY, AlarmMessageType.NO_CAN_VEH);
            Assertions.assertNull(json, "缓存中的不应存在已恢复通知");
        });

        // endregion 连续帧取首帧的数据作为通知数据, 残缺的数据从缓存中取最后有效值.

        // region 持续故障帧不再发出通知

        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 107000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame7 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame7), "第7帧不该出现恢复通知");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 108000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame8 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame8), "第8帧不该出现恢复通知");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 109000, FormatConstant.DATE_FORMAT));
        final Map<String, Object> processFrame9 = carNoCanJudge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(processFrame9), "第9帧不该出现恢复通知");

        // endregion 持续故障帧不再发出通知
    }

    @SuppressWarnings("unused")
    @AfterEach
    private void afterEach() {
        // 每个测试之后
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            jedis.hdel(REDIS_KEY, AlarmMessageType.NO_CAN_VEH);
        });
    }

    @SuppressWarnings("unused")
    @AfterAll
    private static void afterAll() {
        // 所有测试之后
        VEHICLE_CACHE.delFields(TEST_VID);
    }
}
