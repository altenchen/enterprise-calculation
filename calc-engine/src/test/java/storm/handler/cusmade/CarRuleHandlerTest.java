package storm.handler.cusmade;

import com.google.common.collect.Maps;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.SysRealDataCache;
import storm.cache.VehicleCache;
import storm.constant.FormatConstant;
import storm.system.DataKey;
import storm.system.SysDefine;

import java.text.SimpleDateFormat;
import java.util.*;

@Disabled
@DisplayName("SOC过低通知测试和闲置车辆通知")
class CarRuleHandlerTest {


    private static final Logger logger = LoggerFactory.getLogger(CarRuleHandlerTest.class);

    //车辆vid
    private static final String TEST_VID = "TV-" + UUID.randomUUID();

    //redis数据库index
    private static final int REDIS_DB_INDEX = VehicleCache.REDIS_DB_INDEX;
    private static final String REDIS_KEY = VehicleCache.buildRedisKey(TEST_VID);

    //建立全局变量
    private static int socAlarm = 3;
    private static Long lowsocIntervalMillisecond = (long) 0;
    private Map<String, Integer> vidLowSocCount = new HashMap<>();
    private Map<String, Integer> vidNormSoc = new HashMap<>();
    private Map<String, Map<String, Object>> vidSocNotice = new HashMap<>();

    //测试SOC过低通知是否可以正常产生与结束
    @DisplayName("SOC过低通知测试")
    @Test
    void testGenerateNotices_SOC_Fault() {

        final CarRuleHandler CarRuleHandler = new CarRuleHandler();
        //设置只开启soc的规则
        CarRuleHandler.socRule = 1;
        CarRuleHandler.enableCanRule = 0;
        CarRuleHandler.igniteRule = 0;
        CarRuleHandler.gpsRule = 0;
        CarRuleHandler.abnormalRule = 0;
        CarRuleHandler.flyRule = 0;
        CarRuleHandler.onoffRule = 0;
        CarRuleHandler.mileHopRule = 0;
        CarRuleHandler.carLockStatueChangeJudgeRule = 0;

        CarRuleHandler.setLowSocJudgeNum(3);
        CarRuleHandler.setSocAlarm(10);
        CarRuleHandler.setLowSocIntervalMillisecond((long)5000);



        Date date = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        //当前时间
        calendar.add(Calendar.SECOND, -10);
        calendar.getTime();
        date = calendar.getTime();

        final Map<String, String> data = Maps.newTreeMap();
        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey._7615_STATE_OF_CHARGE, "8");
        data.put(DataKey._2502_LONGITUDE,"100");
        data.put(DataKey._2503_LATITUDE,"100");

        //连续三帧soc过低，第三帧产生通知
        Date date1 = new Date(date .getTime() + 1000);
        data.put(DataKey.TIME, DateFormatUtils.format(date1, FormatConstant.DATE_FORMAT));
        List<Map<String, Object>> result_1 = CarRuleHandler.generateNotices(data);
        Assertions.assertTrue(result_1.isEmpty(),"第1帧不该出现故障通知");


        Date date2 = new Date(date .getTime() + 2000);
        data.put(DataKey.TIME, DateFormatUtils.format(date2, FormatConstant.DATE_FORMAT));
        List<Map<String, Object>> result_2 = CarRuleHandler.generateNotices(data);
        Assertions.assertTrue(result_2.isEmpty(),"第2帧不该出现故障通知");


        Date date3 = new Date(date .getTime() + 3000);
        data.put(DataKey.TIME, DateFormatUtils.format(date3, FormatConstant.DATE_FORMAT));
        List<Map<String, Object>> result_3 = CarRuleHandler.generateNotices(data);
        Assertions.assertTrue(0 != result_3.size(),"第3帧出现故障通知");

        data.put(DataKey._7615_STATE_OF_CHARGE, "90");

        //连续三帧soc正常，第三帧发送结束soc过低通知
        Date date4 = new Date(date .getTime() + 1000);
        data.put(DataKey.TIME, DateFormatUtils.format(date4, FormatConstant.DATE_FORMAT));
        List<Map<String, Object>> result_4 = CarRuleHandler.generateNotices(data);
        Assertions.assertTrue(0 != result_4.size(),"第4帧应该该恢复通知");
//
//        Date date5 = new Date(date .getTime() + 2000);
//        data.put(DataKey.TIME, DateFormatUtils.format(date5, FormatConstant.DATE_FORMAT));
//        List<Map<String, Object>> result_5 = CarRuleHandler.generateNotices(data);
//        Assertions.assertTrue(result_5.isEmpty(),"第5帧不该恢复通知");
//
//
//        Date date6 = new Date(date .getTime() + 3000);
//        data.put(DataKey.TIME, DateFormatUtils.format(date6, FormatConstant.DATE_FORMAT));
//        List<Map<String, Object>> result_6 = CarRuleHandler.generateNotices(data);
//        System.out.println(result_6.size());
//        Assertions.assertTrue(0 != result_6.size(),"第6帧恢复通知");

    }

    @DisplayName("闲置车辆通知测试")
    @Test
    void testInidle(){
        
        //闲置车辆通知开始测试
        final CarOnOffHandler CarOnOffHandler = new CarOnOffHandler();
        final Map<String, String> data = Maps.newHashMap();
        long now = System.currentTimeMillis();
        long time = System.currentTimeMillis() - 600000;
        long idleTimeoutMillsecond = 60000;
        now = System.currentTimeMillis();
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        //当前时间减去10分钟
        date.setTime(date.getTime()-20*60*1000);

        String now_time_yyMMddHHmmss_subtract_10minute = sdf.format(date);

        data.put(DataKey.VEHICLE_ID, TEST_VID);
        //数据采集时间，终端采集到数据的时间
        data.put(DataKey.TIME, now_time_yyMMddHHmmss_subtract_10minute);
        data.put(DataKey._2201_SPEED, "40");
        data.put(DataKey._7615_STATE_OF_CHARGE,"66");
        data.put(DataKey._2202_TOTAL_MILEAGE,"18888");
        //系统接收报文时间
        data.put(SysDefine.ONLINE_UTC,now_time_yyMMddHHmmss_subtract_10minute);
        data.put(SysDefine.MESSAGETYPE,"REALTIME");

        SysRealDataCache.addAliveQueue(data.get(DataKey.VEHICLE_ID));
        SysRealDataCache.addLastQueue(data.get(DataKey.VEHICLE_ID));
        SysRealDataCache.livelyCarCache.put(DataKey.VEHICLE_ID,data);
        SysRealDataCache.updateCache(data,time);

        List<Map<String, Object>> notice_start = CarOnOffHandler.fulldoseNotice("TIMEOUT", ScanRange.AllData, now, idleTimeoutMillsecond);
        Assertions.assertTrue(0 != notice_start.size(),"有问题，没有产生闲置开始通知");
        System.out.println(notice_start.get(0).get("smileage"));
        Assertions.assertTrue(notice_start.get(0).get("smileage").equals(18888),"有问题，闲置开始通知中的里程值有问题");

        //闲置车辆通知结束测试

        Date date_now = new Date();
        String now_time_yyMMddHHmmss = sdf.format(date_now);
        //数据采集时间，终端采集到数据的时间
        data.put(DataKey.TIME, now_time_yyMMddHHmmss);
        //系统接收报文时间
        data.put(SysDefine.ONLINE_UTC,now_time_yyMMddHHmmss);
        //闲置车辆通知

        SysRealDataCache.addAliveQueue(data.get(DataKey.VEHICLE_ID));
        SysRealDataCache.addLastQueue(data.get(DataKey.VEHICLE_ID));
        SysRealDataCache.livelyCarCache.put(DataKey.VEHICLE_ID,data);
        SysRealDataCache.updateCache(data,time);

        List<Map<String, Object>> notice_end = CarOnOffHandler.fulldoseNotice("TIMEOUT", ScanRange.AllData, now, idleTimeoutMillsecond);
        Assertions.assertTrue(0 != notice_end.size(),"有问题，没有产生闲置结束通知");

    }


}