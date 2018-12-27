package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleCache;
import storm.constant.FormatConstant;
import storm.protocol.CommandType;
import storm.system.DataKey;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.sql.SQLOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static storm.handler.cusmade.CarMileHopJudge.mileHopRedisKeys;
import static storm.handler.cusmade.CarMileHopJudge.db;

class CarMileHopJudgeTest {
    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CarMileHopJudgeTest.class);

    private static JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static final JsonUtils GSON_UTILS = JsonUtils.getInstance();

    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    private static final String TEST_VID = "TV-" + UUID.randomUUID();

    private static final int REDIS_DB_INDEX = VehicleCache.REDIS_DB_INDEX;
    private static final String REDIS_KEY = VehicleCache.buildRedisKey(TEST_VID);

    private static final String usefulTotalMileage = String.valueOf(
            Math.abs(
                    new Random().nextInt(10000)) + 10000);

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
    }

    @Test
    void processFrame() {
        final long currentTimeMillis = System.currentTimeMillis();

        CarMileHopJudge carMileHopJudge = new CarMileHopJudge();

        final Map<String, String> data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey.MESSAGE_TYPE,CommandType.SUBMIT_REALTIME);

        data.put(DataKey._2202_TOTAL_MILEAGE, "20200");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 101000, FormatConstant.DATE_FORMAT));
        final String processFrame1 = carMileHopJudge.processFrame(data);
        Assertions.assertTrue(StringUtils.isEmpty(processFrame1), "第1帧不该出现里程跳变通知");

        data.put(DataKey._2202_TOTAL_MILEAGE, "20300");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 102000, FormatConstant.DATE_FORMAT));
        final String processFrame2 = carMileHopJudge.processFrame(data);
        System.out.println(processFrame2);
        Assertions.assertTrue(StringUtils.isNotEmpty(processFrame2), "第2帧发生里程跳变");

        data.put(DataKey._2202_TOTAL_MILEAGE, "20310");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 103000, FormatConstant.DATE_FORMAT));
        final String processFrame3 = carMileHopJudge.processFrame(data);
        System.out.println(processFrame3);
        Assertions.assertTrue(StringUtils.isEmpty(processFrame3), "第3帧不应该发生里程跳变通知");
        data.put(DataKey._2202_TOTAL_MILEAGE, "20320");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 104000, FormatConstant.DATE_FORMAT));
        final String processFrame4 = carMileHopJudge.processFrame(data);
        System.out.println(processFrame4);
        Assertions.assertTrue(StringUtils.isEmpty(processFrame4), "第4帧不应该发生里程跳变通知");

        //如果没错，则说明能把reids中的数据读回来。
        String vid_tmp = "TV-5a251171-749c-46a0-80d4-a72a52b2770c";
        String time_tmp = DateFormatUtils.format(currentTimeMillis + 105000, FormatConstant.DATE_FORMAT);
        String nowMileage = "20520";
        saveNowTimeAndMileage(vid_tmp,time_tmp,nowMileage);

        data.put(DataKey.VEHICLE_ID, "TV-5a251171-749c-46a0-80d4-a72a52b2770c");
        data.put(DataKey._2202_TOTAL_MILEAGE, "20620");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 106000, FormatConstant.DATE_FORMAT));
        final String processFrame5 = carMileHopJudge.processFrame(data);
        System.out.println(processFrame5);
        Assertions.assertTrue(StringUtils.isNotEmpty(processFrame5), "第5帧应该发生里程跳变通知");

    }
    /**
     * 保存“时间”和“里程值”到缓存和redis中
     * @param vid 车辆id
     * @param time 最近一帧有效里程值的服务器接收报文时间
     * @param nowMileage 最近一帧有效里程值
     */
    private void saveNowTimeAndMileage(String vid, String time, String nowMileage){
        Map<String, String> usefulTimeAndMileage = new HashMap<>();
        usefulTimeAndMileage.put("time",time);
        usefulTimeAndMileage.put("lastUsefulMileage",nowMileage);

        final String json = JSON_UTILS.toJson(usefulTimeAndMileage);
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(db);
            jedis.hset(mileHopRedisKeys, vid, json);
        });
    }

}