package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;
import storm.extension.ObjectExtension;
import storm.system.DataKey;
import storm.util.ConfigUtils;
import storm.util.JsonUtils;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@DisplayName("点火熄火最大车速测试")
public class CarIgniteShutJudgeTest {
    private static final Logger LOG = LoggerFactory.getLogger(CarIgniteShutJudgeTest.class);


    private static final String TEST_VID = "TV-" + UUID.randomUUID();

    private static final String TEST_TIME = DateFormatUtils.format(
        System.currentTimeMillis(),
        FormatConstant.DATE_FORMAT);

    private static final Type TREE_MAP_STRING_STRING_TYPE = new TypeToken<TreeMap<String, String>>() {
    }.getType();
    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    @Test
    public void maxSpeedTest() {
        ConfigUtils.getSysDefine().setNoticeIgniteTriggerContinueCount(2);
        ConfigUtils.getSysDefine().setNoticeIgniteTriggerTimeoutMillisecond(0);
        ConfigUtils.getSysDefine().setNoticeShutTriggerContinueCount(1);
        ConfigUtils.getSysDefine().setNoticeShutTriggerTimeoutMillisecond(0);

        CarIgniteShutJudge judge = new CarIgniteShutJudge();

        final Map<String, String> data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey.VEHICLE_NUMBER, TEST_VID);
        data.put(DataKey._2502_LONGITUDE, "100");
        data.put(DataKey._2503_LATITUDE, "100");
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, TEST_TIME);
        data.put(DataKey.TIME, TEST_TIME);


        //车辆熄火
        data.put(DataKey._3201_CAR_STATUS, "2");
        data.put(DataKey._2201_SPEED, "720");
        String notice1 = judge.processFrame(TEST_VID, ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(notice1), "第1帧不该出现通知");

        //车辆点火
        data.put(DataKey._3201_CAR_STATUS, "1");
        data.put(DataKey._2201_SPEED, "190");
        String notice2 = judge.processFrame(TEST_VID, ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(notice2), "第2帧不该出现通知");

        data.put(DataKey._2201_SPEED, "220");
        String notice3 = judge.processFrame(TEST_VID, ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isNotBlank(notice3), "第3帧发出点火通知");
        LOG.info("notice : {}", notice3);

        data.put(DataKey._2201_SPEED, "330");
        String notice4 = judge.processFrame(TEST_VID, ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(notice4), "第4帧不该出现通知");

        //车辆熄火
        data.put(DataKey._3201_CAR_STATUS, "2");
        data.put(DataKey._2201_SPEED, "0");
        String notice5 = judge.processFrame(TEST_VID, ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isNotBlank(notice5), "第5帧出现熄火通知");
        LOG.info("notice : {}", notice5);

        ImmutableMap<String, String> notice5Map = convertJson(notice5);
        Assertions.assertEquals(330d, stringToDouble(notice5Map.get("maxSpeed")), "最大里程统计错误");


    }

    private ImmutableMap<String, String> convertJson(String json) {
        return ImmutableMap.copyOf(ObjectExtension.defaultIfNull(
            JSON_UTILS.fromJson(
                json,
                TREE_MAP_STRING_STRING_TYPE,
                e -> null),
            Maps::newTreeMap));
    }

    private double stringToDouble(String str) {
        if (NumberUtils.isNumber(str)) {
            return Double.valueOf(str);
        }
        return 0;
    }


}
