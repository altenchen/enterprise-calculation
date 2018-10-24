package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import storm.constant.FormatConstant;
import storm.system.DataKey;
import java.util.*;

class CarLowSocJudgeTest {
    //vid
    private static final String TEST_VID = "TV-" + UUID.randomUUID();

    @Test
    void testProcessFrame() {

        CarLowSocJudge carLowSocJudge = new CarLowSocJudge();

        Date date = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.SECOND, -10);
        calendar.getTime();
        date = calendar.getTime();

        final Map<String, String> data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey._7615_STATE_OF_CHARGE, "8");
        data.put(DataKey._2502_LONGITUDE,"100");
        data.put(DataKey._2503_LATITUDE,"100");



        Date date1 = new Date(date .getTime() - 1003000);
        data.put(DataKey.TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        List<Map<String, Object>> result_1 = carLowSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(0 == result_1.size(),"1不应该出现通知");

        Date date2 = new Date(date .getTime() - 1002000);
        data.put(DataKey.TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        List<Map<String, Object>> result_2 = carLowSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(0 == result_2.size(),"2不应该出现通知");

        Date date3 = new Date(date .getTime() - 1001000);
        data.put(DataKey.TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        List<Map<String, Object>> result_3 = carLowSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(0 != result_3.size(),"3应该出现通知֪");

        //大于soc过低通知开始阈值，小于soc过低通知结束阈值
        data.put(DataKey._7615_STATE_OF_CHARGE, "15");
        Date date4 = new Date(date .getTime() );
        data.put(DataKey.TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        List<Map<String, Object>> result_4 = carLowSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(0 == result_4.size(),"4不应该出现通知");

        //大于soc过低通知结束阈值
        data.put(DataKey._7615_STATE_OF_CHARGE, "21");
        Date date5 = new Date(date .getTime() + 1000);
        data.put(DataKey.TIME, DateFormatUtils.format(date5, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date5, FormatConstant.DATE_FORMAT));
        List<Map<String, Object>> result_5 = carLowSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(0 != result_5.size(),"5应该出现通知");

    }
}
