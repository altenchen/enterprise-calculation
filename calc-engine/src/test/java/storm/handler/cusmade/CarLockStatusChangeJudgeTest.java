package storm.handler.cusmade;

import com.google.common.collect.ImmutableMap;
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
