package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;
import storm.protocol.CommandType;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.system.ProtocolItem;
import storm.util.JedisPoolUtils;
import storm.util.JsonUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static storm.cache.VehicleCache.REDIS_DB_INDEX;

class CarOnOffMileJudgeTest {
    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();
    private static JedisPoolUtils JEDIS_POOL_UTILS = JedisPoolUtils.getInstance();
    private static int REDIS_DB_INDEX = 6;
    private static String onOffMileRedisKeys = "vehCache.qy.onOffMile.notice";

    private static final String TEST_VID = "TV-" + UUID.randomUUID();
    @Test
    void processFrame() {
        final long currentTimeMillis = System.currentTimeMillis();

        CarOnOffMileJudge carOnOffMileJudge = new CarOnOffMileJudge();

        Map<String, String> data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey.MESSAGE_TYPE,CommandType.SUBMIT_REALTIME);

        data.put(DataKey._2202_TOTAL_MILEAGE, "10010");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 10000, FormatConstant.DATE_FORMAT));
        ImmutableMap data1 = ImmutableMap.copyOf(data);
        final String processFrame1 = carOnOffMileJudge.processFrame(data1);
        Assertions.assertTrue(StringUtils.isEmpty(processFrame1), "第1帧不该出现上下线里程通知");

        data.put(DataKey._2202_TOTAL_MILEAGE, "10020");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 20000, FormatConstant.DATE_FORMAT));
        ImmutableMap data2 = ImmutableMap.copyOf(data);
        final String processFrame2 = carOnOffMileJudge.processFrame(data2);
        Assertions.assertTrue(StringUtils.isEmpty(processFrame2), "第2帧不该出现上下线里程通知");

        //1、测试车辆下线然后上线，是不是能产生上下线里程通知。
        data.put(DataKey._2202_TOTAL_MILEAGE, "10030");
        data.put(DataKey.MESSAGE_TYPE,CommandType.SUBMIT_LOGIN);
        data.put(ProtocolItem.REG_TYPE,"2");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 30000, FormatConstant.DATE_FORMAT));
        ImmutableMap data3 = ImmutableMap.copyOf(data);
        final String processFrame3 = carOnOffMileJudge.processFrame(data3);
        Assertions.assertTrue(StringUtils.isEmpty(processFrame3), "第3帧不该出现上下线里程通知");

        data.put(DataKey._2202_TOTAL_MILEAGE, "10040");
        data.put(DataKey.MESSAGE_TYPE,CommandType.SUBMIT_LOGIN);
        data.put(ProtocolItem.REG_TYPE,"1");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 40000, FormatConstant.DATE_FORMAT));
        ImmutableMap data4 = ImmutableMap.copyOf(data);
        final String processFrame4 = carOnOffMileJudge.processFrame(data4);
        Assertions.assertTrue(!StringUtils.isEmpty(processFrame4), "第4帧应该出现上下线里程通知");

        //2、测试车辆下线，然后发一条实时报文，是不是能产生上下线里程通知。
        data.put(DataKey._2202_TOTAL_MILEAGE, "10050");
        data.put(DataKey.MESSAGE_TYPE,CommandType.SUBMIT_LOGIN);
        data.put(ProtocolItem.REG_TYPE,"2");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 50000, FormatConstant.DATE_FORMAT));
        ImmutableMap data5 = ImmutableMap.copyOf(data);
        final String processFrame5 = carOnOffMileJudge.processFrame(data5);
        Assertions.assertTrue(StringUtils.isEmpty(processFrame5), "第5帧不该出现上下线里程通知");

        data.put(DataKey._2202_TOTAL_MILEAGE, "10060");
        data.put(DataKey.MESSAGE_TYPE,CommandType.SUBMIT_REALTIME);
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 60000, FormatConstant.DATE_FORMAT));
        ImmutableMap data6 = ImmutableMap.copyOf(data);
        final String processFrame6 = carOnOffMileJudge.processFrame(data6);
        Assertions.assertTrue(!StringUtils.isEmpty(processFrame6), "第6帧不该出现上下线里程通知");


        //3、如果没错，则说明能把reids中的数据读回来。
        String vid_tmp = "TV-5a251171-749c-46a0-80d4-a72a52b2770c";
        String time_tmp = DateFormatUtils.format(currentTimeMillis + 70000, FormatConstant.DATE_FORMAT);
        String nowMileage = "10070";
        saveOnOffMileNotice(vid_tmp,time_tmp,nowMileage);

        data.put(DataKey.VEHICLE_ID, "TV-5a251171-749c-46a0-80d4-a72a52b2770c");
        data.put(DataKey._2202_TOTAL_MILEAGE, "10080");
        data.put(DataKey.TIME, DateFormatUtils.format(currentTimeMillis + 80000, FormatConstant.DATE_FORMAT));
        ImmutableMap data7 = ImmutableMap.copyOf(data);
        final String processFrame7 = carOnOffMileJudge.processFrame(data7);
        System.out.println(processFrame7);
        Assertions.assertTrue(StringUtils.isNotEmpty(processFrame7), "第7帧应该出现上下线里程通知");

    }
    private void saveOnOffMileNotice(String vid, String time, String nowMileage){

        Map<String, String> notice = new HashMap<>();
        notice.put("msgType", NoticeType.ON_OFF_MILE);
        notice.put("vid", vid);
        notice.put("stime", time);
        notice.put("smileage", nowMileage);

        final String json = JSON_UTILS.toJson(notice);
        JEDIS_POOL_UTILS.useResource(jedis -> {
            jedis.select(REDIS_DB_INDEX);
            jedis.hset(onOffMileRedisKeys, vid, json);
        });
    }
}