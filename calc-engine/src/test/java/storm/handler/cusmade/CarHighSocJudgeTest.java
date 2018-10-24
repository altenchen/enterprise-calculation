package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import storm.constant.FormatConstant;
import storm.system.DataKey;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class CarHighSocJudgeTest {
    //车辆vid
    private static final String TEST_VID = "TV-" + UUID.randomUUID();

    @Test
    void testProcessFrame() {

        CarHighSocJudge carHighSocJudge = new CarHighSocJudge();

        Date date = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        //当前时间
        calendar.add(Calendar.SECOND, -10);
        calendar.getTime();
        date = calendar.getTime();

        final Map<String, String> data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey._7615_STATE_OF_CHARGE, "99");
        data.put(DataKey._2502_LONGITUDE, "100");
        data.put(DataKey._2503_LATITUDE, "100");


        Date date1 = new Date(date.getTime() - 1003000);
        data.put(DataKey.TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        List<Map<String, String>> result_1 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(0 == result_1.size(), "第1帧不该出现故障通知");

        Date date2 = new Date(date.getTime() - 1002000);
        data.put(DataKey.TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        List<Map<String, String>> result_2 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(0 == result_2.size(), "第2帧不该出现故障通知");

        Date date3 = new Date(date.getTime() - 1001000);
        data.put(DataKey.TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        List<Map<String, String>> result_3 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(0 != result_3.size(), "第3帧出现故障通知");

        data.put(DataKey._7615_STATE_OF_CHARGE, "85");
        Date date4 = new Date(date.getTime());
        data.put(DataKey.TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        List<Map<String, String>> result_4 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(0 == result_4.size(), "第4帧不应该故障结束");

        data.put(DataKey._7615_STATE_OF_CHARGE, "79");
        Date date5 = new Date(date.getTime() + 1000);
        data.put(DataKey.TIME, DateFormatUtils.format(date5, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date5, FormatConstant.DATE_FORMAT));
        List<Map<String, String>> result_5 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(0 != result_5.size(), "第5帧故障结束");

    }
}