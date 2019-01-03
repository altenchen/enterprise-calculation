package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.conf.SysDefineEntity;
import storm.constant.FormatConstant;
import storm.system.DataKey;
import storm.util.ConfigUtils;

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

class CarLowSocJudgeTest {
    private static final Logger LOG = LoggerFactory.getLogger(CarLowSocJudgeTest.class);

    //vid
    private static final String TEST_VID = "TV-" + UUID.randomUUID();

    @Test
    void testProcessFrame() {

        final SysDefineEntity sysDefine = ConfigUtils.getSysDefine();
        sysDefine.setNoticeSocLowBeginTriggerThreshold(10);
        sysDefine.setNoticeSocLowBeginTriggerContinueCount(3);
        sysDefine.setNoticeSocLowBeginTriggerTimeoutMillisecond(30000);
        sysDefine.setNoticeSocLowEndTriggerThreshold(20);
        sysDefine.setNoticeSocLowEndTriggerContinueCount(1);
        sysDefine.setNoticeSocLowEndTriggerTimeoutMillisecond(0);

        CarLowSocJudge carLowSocJudge = new CarLowSocJudge();

        final Map<String, String> data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey._7615_STATE_OF_CHARGE, "8");
        data.put(DataKey._2502_LONGITUDE,"100");
        data.put(DataKey._2503_LATITUDE,"100");

        Date date1 = new Date(TimeUnit.MINUTES.toMillis(10));
        data.put(DataKey.TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        String result_1 = carLowSocJudge.processFrame(ImmutableMap.copyOf(data), () -> {});
        Assertions.assertTrue(StringUtils.isBlank(result_1),"1不应该出现通知");

        Date date2 = new Date(TimeUnit.MINUTES.toMillis(20));
        data.put(DataKey.TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        String result_2 = carLowSocJudge.processFrame(ImmutableMap.copyOf(data), () -> {});
        Assertions.assertTrue(StringUtils.isBlank(result_2),"2不应该出现通知");

        Date date3 = new Date(TimeUnit.MINUTES.toMillis(30));
        data.put(DataKey.TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        String result_3 = carLowSocJudge.processFrame(ImmutableMap.copyOf(data), () -> {});
        Assertions.assertTrue(StringUtils.isNotBlank(result_3),"3应该出现通知֪");
        LOG.info("notice : {}", result_3);

        //大于soc过低通知开始阈值，小于soc过低通知结束阈值
        data.put(DataKey._7615_STATE_OF_CHARGE, "15");
        Date date4 =  new Date(TimeUnit.MINUTES.toMillis(40));
        data.put(DataKey.TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        String result_4 = carLowSocJudge.processFrame(ImmutableMap.copyOf(data), () -> {});
        Assertions.assertTrue(StringUtils.isBlank(result_4),"4不应该出现通知");

        //大于soc过低通知结束阈值
        data.put(DataKey._7615_STATE_OF_CHARGE, "21");
        Date date5 =  new Date(TimeUnit.MINUTES.toMillis(50));
        data.put(DataKey.TIME, DateFormatUtils.format(date5, FormatConstant.DATE_FORMAT));
        data.put(DataKey._9999_PLATFORM_RECEIVE_TIME, DateFormatUtils.format(date5, FormatConstant.DATE_FORMAT));
        String result_5 = carLowSocJudge.processFrame(ImmutableMap.copyOf(data), () -> {});
        Assertions.assertTrue(StringUtils.isNotBlank(result_5),"5应该出现通知");
        LOG.info("notice : {}", result_5);

    }
}
