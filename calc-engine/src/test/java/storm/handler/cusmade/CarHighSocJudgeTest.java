package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import storm.constant.FormatConstant;
import storm.system.DataKey;
import storm.util.ConfigUtils;

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * SOC过高单元测试
 * @author xzj
 */
class CarHighSocJudgeTest {

    //车辆vid
    private static String TEST_VID = "TV-" + UUID.randomUUID();

    //车辆实时数据
    private static Map<String, String> data;

    //测试对象
    private static CarHighSocJudge carHighSocJudge;

    /**
     * 重置测试对象
     * @return
     */
    private void reset(){
        //初始化测试对象
        carHighSocJudge = new CarHighSocJudge();
        ConfigUtils.getSysDefine().setNoticeSocHighBeginTriggerThreshold(90);
        ConfigUtils.getSysDefine().setNoticeSocHighBeginTriggerContinueCount(3);
        ConfigUtils.getSysDefine().setNoticeSocHighBeginTriggerTimeoutMillisecond(30000);
        ConfigUtils.getSysDefine().setNoticeSocHighEndTriggerThreshold(80);
        ConfigUtils.getSysDefine().setNoticeSocHighEndTriggerContinueCount(1);
        ConfigUtils.getSysDefine().setNoticeSocHighEndTriggerTimeoutMillisecond(0);


        //初始化车辆基础数据
        data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey._7615_STATE_OF_CHARGE, "99");
        data.put(DataKey._2502_LONGITUDE, "100");
        data.put(DataKey._2503_LATITUDE, "100");
    }

    /**
     * 正常流程测试
     */
    @Test
    void 正常流程测试() {
        reset();

        Date date1 = new Date(TimeUnit.MINUTES.toMillis(10));
        data.put(DataKey.TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        String result_1 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(result_1),"第1帧不该出现故障通知");

        Date date2 = new Date(TimeUnit.MINUTES.toMillis(20));
        data.put(DataKey.TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        String result_2 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(result_2),"第2帧不该出现故障通知");

        Date date3 = new Date(TimeUnit.MINUTES.toMillis(30));
        data.put(DataKey.TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        String result_3 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isNotBlank(result_3),"第3帧出现故障通知");

        Date date3_1 = new Date(TimeUnit.MINUTES.toMillis(40));
        data.put(DataKey.TIME, DateFormatUtils.format(date3_1, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date3_1, FormatConstant.DATE_FORMAT));
        String result_3_1 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isEmpty(result_3_1),"第3_1帧不该出现故障通知");

        data.put(DataKey._7615_STATE_OF_CHARGE, "85");
        Date date4 =  new Date(TimeUnit.MINUTES.toMillis(50));
        data.put(DataKey.TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        String result_4 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(result_4),"第4帧不应该故障结束");

        data.put(DataKey._7615_STATE_OF_CHARGE, "79");
        Date date5 =  new Date(TimeUnit.MINUTES.toMillis(60));
        data.put(DataKey.TIME, DateFormatUtils.format(date5, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date5, FormatConstant.DATE_FORMAT));
        String result_5 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isNotBlank(result_5),"第5帧故障结束");

    }

    @Test
    void 没有达到时间阀值测试() {
        reset();
        Date date1 = new Date(TimeUnit.MINUTES.toMillis(10));
        data.put(DataKey.TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        String result_1 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(result_1),"第1帧不该出现故障通知");

        Date date2 = new Date(TimeUnit.MINUTES.toSeconds(11));
        data.put(DataKey.TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        String result_2 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(result_2),"第2帧不该出现故障通知");

        Date date3 = new Date(TimeUnit.MINUTES.toSeconds(12));
        data.put(DataKey.TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        String result_3 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(result_3),"第3帧不该出现故障通知");

        Date date4 = new Date(TimeUnit.MINUTES.toMillis(50));
        data.put(DataKey.TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        String result_4 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isNotBlank(result_4),"第4帧出现故障通知");

        Date date5 = new Date(TimeUnit.MINUTES.toMillis(60));
        data.put(DataKey.TIME, DateFormatUtils.format(date5, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date5, FormatConstant.DATE_FORMAT));
        String result_5 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isEmpty(result_5),"第5帧不该出现故障通知");

    }

    @Test
    void SOC没有达到阀值测试() {
        reset();

        data.put(DataKey._7615_STATE_OF_CHARGE, "88");
        Date date1 = new Date(TimeUnit.MINUTES.toMillis(10));
        data.put(DataKey.TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        String result_1 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(result_1),"第1帧不该出现故障通知");

        data.put(DataKey._7615_STATE_OF_CHARGE, "85");
        Date date2 = new Date(TimeUnit.MINUTES.toMillis(20));
        data.put(DataKey.TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        String result_2 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(result_2),"第2帧不该出现故障通知");

        data.put(DataKey._7615_STATE_OF_CHARGE, "80");
        Date date3 = new Date(TimeUnit.MINUTES.toMillis(30));
        data.put(DataKey.TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        String result_3 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isEmpty(result_3),"第3帧不该出现故障通知");

        data.put(DataKey._7615_STATE_OF_CHARGE, "75");
        Date date4 =  new Date(TimeUnit.MINUTES.toMillis(40));
        data.put(DataKey.TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        String result_4 = carHighSocJudge.processFrame(ImmutableMap.copyOf(data));
        Assertions.assertTrue(StringUtils.isBlank(result_4),"第4帧不应该故障结束");

    }
}