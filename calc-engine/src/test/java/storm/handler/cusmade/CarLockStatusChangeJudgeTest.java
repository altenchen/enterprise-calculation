package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.cache.VehicleCache;
import storm.constant.FormatConstant;
import storm.protocol.CommandType;
import storm.system.DataKey;
import storm.system.NoticeType;
import storm.system.SysDefine;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author: xzp
 * @date: 2018-07-28
 * @description:
 */
@DisplayName("CarLockStatusChangeJudgeTest")
public class CarLockStatusChangeJudgeTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CarLockStatusChangeJudgeTest.class);
    private static final VehicleCache VEHICLE_CACHE = VehicleCache.getInstance();

    private static final String TEST_VID = "TV-" + UUID.randomUUID();
    private static final String TEST_TIME = DateFormatUtils.format(
        System.currentTimeMillis(),
        FormatConstant.DATE_FORMAT);

    private static final ImmutableMap DISABLE_UNLOCK = new ImmutableMap.Builder<String, Object>()
        .put(DataKey.VEHICLE_ID, TEST_VID)
        .put(DataKey.TIME, TEST_TIME)
        .put(SysDefine.MESSAGETYPE, CommandType.SUBMIT_REALTIME)
        .put(DataKey._4710061_JILI_LOCK_FUNCTION, DataKey._4710061_JILI_LOCK_FUNCTION_DISABLE)
        .put(DataKey._4710062_JILI_LOCK_STATUS, DataKey._4710062_JILI_LOCK_STATUS_UNLOCK)
        .build();
    private static final ImmutableMap DISABLE_LOCKED = new ImmutableMap.Builder<String, Object>()
        .put(DataKey.VEHICLE_ID, TEST_VID)
        .put(DataKey.TIME, TEST_TIME)
        .put(SysDefine.MESSAGETYPE, CommandType.SUBMIT_REALTIME)
        .put(DataKey._4710061_JILI_LOCK_FUNCTION, DataKey._4710061_JILI_LOCK_FUNCTION_DISABLE)
        .put(DataKey._4710062_JILI_LOCK_STATUS, DataKey._4710062_JILI_LOCK_STATUS_LOCKED)
        .build();
    private static final ImmutableMap ENABLE_UNLOCK = new ImmutableMap.Builder<String, Object>()
        .put(DataKey.VEHICLE_ID, TEST_VID)
        .put(DataKey.TIME, TEST_TIME)
        .put(SysDefine.MESSAGETYPE, CommandType.SUBMIT_REALTIME)
        .put(DataKey._4710061_JILI_LOCK_FUNCTION, DataKey._4710061_JILI_LOCK_FUNCTION_ENABLE)
        .put(DataKey._4710062_JILI_LOCK_STATUS, DataKey._4710062_JILI_LOCK_STATUS_UNLOCK)
        .build();
    private static final ImmutableMap ENABLE_LOCKED = new ImmutableMap.Builder<String, Object>()
        .put(DataKey.VEHICLE_ID, TEST_VID)
        .put(DataKey.TIME, TEST_TIME)
        .put(SysDefine.MESSAGETYPE, CommandType.SUBMIT_REALTIME)
        .put(DataKey._4710061_JILI_LOCK_FUNCTION, DataKey._4710061_JILI_LOCK_FUNCTION_ENABLE)
        .put(DataKey._4710062_JILI_LOCK_STATUS, DataKey._4710062_JILI_LOCK_STATUS_LOCKED)
        .build();

    private CarLockStatusChangeJudgeTest() {
    }

    @SuppressWarnings("unused")
    @BeforeAll
    private static void beforeAll() {
        // 所有测试之前
    }

    @SuppressWarnings("unused")
    @BeforeEach
    private void beforeEach() {
        // 每个测试之前
        VEHICLE_CACHE.delFields(TEST_VID);
    }

    //（车辆动力锁止功能状态，车辆动力锁止状态）应该只有3种组合（0,2）（1,0）（1,1）
    @DisplayName("于心沼综合测试")
    @Test
    void carLockStatueChangeJudge() {
        final Map<String, String> data = Maps.newHashMap();
        Date date = new Date();

        data.put(DataKey.VEHICLE_ID, TEST_VID);
        data.put(DataKey.TIME, DateFormatUtils.format(date, FormatConstant.DATE_FORMAT));
        data.put(SysDefine.MESSAGETYPE,CommandType.SUBMIT_REALTIME );
        data.put(DataKey._4710061_JILI_LOCK_FUNCTION,"0");
        data.put(DataKey._4710062_JILI_LOCK_STATUS,"2");

        CarLockStatusChangeJudge  lock = new CarLockStatusChangeJudge();
        Map<String, Object> notice_1 = lock.carLockStatueChangeJudge(data);
        Assertions.assertTrue(null==notice_1,"第一帧不应该生成notice");

        data.put(DataKey._4710061_JILI_LOCK_FUNCTION,"1");
        data.put(DataKey._4710062_JILI_LOCK_STATUS,"0");
        Map<String, Object> notice_2 = lock.carLockStatueChangeJudge(data);
        System.out.println(notice_2.size());
        Assertions.assertTrue(0 != notice_2.size(),"第二帧应该生成notice");

        data.put(DataKey._4710061_JILI_LOCK_FUNCTION,"1");
        data.put(DataKey._4710062_JILI_LOCK_STATUS,"1");
        Map<String, Object> notice_3 = lock.carLockStatueChangeJudge(data);
        Assertions.assertTrue(0 != notice_3.size(),"第三帧应该生成notice");

        data.put(DataKey._4710061_JILI_LOCK_FUNCTION,"0");
        data.put(DataKey._4710062_JILI_LOCK_STATUS,"1");
        Map<String, Object> notice_4 = lock.carLockStatueChangeJudge(data);
        System.out.println(notice_4.get("lockFunctionStatusChange"));
        System.out.println(notice_4.get("lockStatusChange"));
        Assertions.assertTrue("0".equals(notice_4.get("lockFunctionStatusChange")),"锁止功能状态应该为0，关闭");
        Assertions.assertTrue("2".equals(notice_4.get("lockStatusChange")),"因为锁止功能状态为0，所以，虽然这帧报文的车辆锁止状态为1，已锁止，但是仍然应该为2，禁止锁止。这种情况一般不会出现");

        data.put(DataKey._4710061_JILI_LOCK_FUNCTION,"3");
        data.put(DataKey._4710062_JILI_LOCK_STATUS,"4");
        Map<String, Object> notice_5 = lock.carLockStatueChangeJudge(data);
        Assertions.assertTrue(null == notice_5,"状态不合法，notice应该返回null");

    }

    @DisplayName("禁用未锁")
    @Test
    void disableUnlock() {
        final CarLockStatusChangeJudge carLockStatusChangeJudge = new CarLockStatusChangeJudge();

        final Map<String, Object> notice = carLockStatusChangeJudge.carLockStatueChangeJudge(DISABLE_UNLOCK);
        Assertions.assertEquals(NoticeType.VEH_LOCK_STATUS_CHANGE, ObjectUtils.toString(notice.get("msgType")));
        Assertions.assertEquals(TEST_VID, ObjectUtils.toString(notice.get("vid")));
        Assertions.assertEquals(DataKey._4710061_JILI_LOCK_FUNCTION_DISABLE, ObjectUtils.toString(notice.get("lockFunctionStatusChange")));
        Assertions.assertEquals(DataKey._4710062_JILI_LOCK_STATUS_DISABLE, ObjectUtils.toString(notice.get("lockStatusChange")));

        final Map<String, Object> again = carLockStatusChangeJudge.carLockStatueChangeJudge(DISABLE_UNLOCK);
        Assertions.assertTrue(MapUtils.isEmpty(again));
    }

    @DisplayName("禁用已锁")
    @Test
    void disableLocked() {
        final CarLockStatusChangeJudge carLockStatusChangeJudge = new CarLockStatusChangeJudge();

        final Map<String, Object> notice = carLockStatusChangeJudge.carLockStatueChangeJudge(DISABLE_LOCKED);
        Assertions.assertEquals(NoticeType.VEH_LOCK_STATUS_CHANGE, ObjectUtils.toString(notice.get("msgType")));
        Assertions.assertEquals(TEST_VID, ObjectUtils.toString(notice.get("vid")));
        Assertions.assertEquals(DataKey._4710061_JILI_LOCK_FUNCTION_DISABLE, ObjectUtils.toString(notice.get("lockFunctionStatusChange")));
        Assertions.assertEquals(DataKey._4710062_JILI_LOCK_STATUS_DISABLE, ObjectUtils.toString(notice.get("lockStatusChange")));

        final Map<String, Object> again = carLockStatusChangeJudge.carLockStatueChangeJudge(DISABLE_LOCKED);
        Assertions.assertTrue(MapUtils.isEmpty(again));
    }

    @DisplayName("启用未锁")
    @Test
    void enableUnlock() {
        final CarLockStatusChangeJudge carLockStatusChangeJudge = new CarLockStatusChangeJudge();

        final Map<String, Object> notice = carLockStatusChangeJudge.carLockStatueChangeJudge(ENABLE_UNLOCK);
        Assertions.assertEquals(NoticeType.VEH_LOCK_STATUS_CHANGE, ObjectUtils.toString(notice.get("msgType")));
        Assertions.assertEquals(TEST_VID, ObjectUtils.toString(notice.get("vid")));
        Assertions.assertEquals(DataKey._4710061_JILI_LOCK_FUNCTION_ENABLE, ObjectUtils.toString(notice.get("lockFunctionStatusChange")));
        Assertions.assertEquals(DataKey._4710062_JILI_LOCK_STATUS_UNLOCK, ObjectUtils.toString(notice.get("lockStatusChange")));

        final Map<String, Object> again = carLockStatusChangeJudge.carLockStatueChangeJudge(ENABLE_UNLOCK);
        Assertions.assertTrue(MapUtils.isEmpty(again));
    }

    @DisplayName("启用已锁")
    @Test
    void enableLocked() {
        final CarLockStatusChangeJudge carLockStatusChangeJudge = new CarLockStatusChangeJudge();

        final Map<String, Object> notice = carLockStatusChangeJudge.carLockStatueChangeJudge(ENABLE_LOCKED);
        Assertions.assertEquals(NoticeType.VEH_LOCK_STATUS_CHANGE, ObjectUtils.toString(notice.get("msgType")));
        Assertions.assertEquals(TEST_VID, ObjectUtils.toString(notice.get("vid")));
        Assertions.assertEquals(DataKey._4710061_JILI_LOCK_FUNCTION_ENABLE, ObjectUtils.toString(notice.get("lockFunctionStatusChange")));
        Assertions.assertEquals(DataKey._4710062_JILI_LOCK_STATUS_LOCKED, ObjectUtils.toString(notice.get("lockStatusChange")));

        final Map<String, Object> again = carLockStatusChangeJudge.carLockStatueChangeJudge(ENABLE_LOCKED);
        Assertions.assertTrue(MapUtils.isEmpty(again));
    }

    @SuppressWarnings("unused")
    @AfterEach
    private void afterEach() {
        // 每个测试之后
    }

    @SuppressWarnings("unused")
    @AfterAll
    private static void afterAll() {
        // 所有测试之后
        VEHICLE_CACHE.delFields(TEST_VID);
    }
}
