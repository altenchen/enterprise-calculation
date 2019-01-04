package storm.handler.cusmade;

import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import storm.constant.FormatConstant;
import storm.system.DataKey;

import java.util.Map;
import java.util.UUID;

@DisplayName("点火熄火最大车速测试")
public class CarIgniteShutJudgeTest {

    private static final String TEST_VID = "TV-" + UUID.randomUUID();

    private static final String TEST_TIME = DateFormatUtils.format(
        System.currentTimeMillis(),
        FormatConstant.DATE_FORMAT);

    @Test
    public void maxSpeedTest(){
        CarIgniteShutJudge judge = new CarIgniteShutJudge();

        final Map<String, String> data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey._2502_LONGITUDE, "100");
        data.put(DataKey._2503_LATITUDE, "100");
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, TEST_TIME);
        data.put(DataKey.TIME, TEST_TIME);


        //车辆熄火
        data.put(DataKey._3201_CAR_STATUS, "2");
        data.put(DataKey._2201_SPEED, "720");
        Map<String, Object> notice1 = judge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(notice1), "第1帧不该出现通知");

        //车辆点火
        data.put(DataKey._3201_CAR_STATUS, "1");
        data.put(DataKey._2201_SPEED, "220");
        Map<String, Object> notice2 = judge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(notice2), "第2帧不该出现通知");

        data.put(DataKey._2201_SPEED, "190");
        Map<String, Object> notice3 = judge.processFrame(data);
        Assertions.assertTrue(MapUtils.isNotEmpty(notice3), "第3帧发出点火通知");

        data.put(DataKey._2201_SPEED, "330");
        Map<String, Object> notice4 = judge.processFrame(data);
        Assertions.assertTrue(MapUtils.isEmpty(notice4), "第4帧不该出现通知");

        //车辆熄火
        data.put(DataKey._3201_CAR_STATUS, "2");
        Map<String, Object> notice5 = judge.processFrame(data);
        Assertions.assertTrue(MapUtils.isNotEmpty(notice5), "第5帧出现熄火通知");

        Assertions.assertEquals(330d, notice5.get("maxSpeed"), "最大里程统计错误");


    }


}
