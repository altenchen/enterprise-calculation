package storm.domain.fence.notice;

import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.Fence;
import storm.domain.fence.area.AreaSide;
import storm.domain.fence.area.Coordinate;
import storm.domain.fence.event.Event;
import storm.domain.fence.event.EventStage;
import storm.util.DataUtils;
import storm.util.JsonUtils;

import java.text.ParseException;
import java.util.UUID;

/**
 * @author: xzp
 * @date: 2018-12-06
 * @description:
 */
@DisplayName("电子围栏通知测试")
final class FenceNoticeTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(FenceNoticeTest.class);

    private static final JsonUtils JSON_UTILS = JsonUtils.getInstance();

    @NotNull
    private static final String messageId = UUID.randomUUID().toString();

    @NotNull
    private static final String fenceId = UUID.randomUUID().toString();

    @NotNull
    private static final String eventId = UUID.randomUUID().toString();

    @NotNull
    private static final String vehicleId = UUID.randomUUID().toString();

    @NotNull
    private static final String dataTime = DataUtils.buildFormatTime(System.currentTimeMillis());

    private static final double longitude = 12.34;

    private static final double latitude = 56.78;

    @NotNull
    private static final String noticeTime = dataTime;

    private FenceNoticeTest() {
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
    }

    @Disabled("打印驶入开始通知")
    @Test
    void printBeginInsideNoticeJson() {
        @NotNull final AreaSide fromArea = AreaSide.OUTSIDE;
        @NotNull final AreaSide gotoArea = AreaSide.INSIDE;
        @NotNull final EventStage eventStage = EventStage.BEGIN;

        final DriveInsideNotice notice = new DriveInsideNotice(
            messageId,
            fenceId,
            eventId,
            vehicleId,
            dataTime,
            longitude,
            latitude,
            fromArea,
            gotoArea,
            eventStage,
            noticeTime
        );

        final String json = JSON_UTILS.toJson(notice);

        LOG.trace(json);
    }

    @Disabled("打印驶入结束通知")
    @Test
    void printEndInsideNoticeJson() {

        @NotNull final AreaSide fromArea = AreaSide.INSIDE;
        @NotNull final AreaSide gotoArea = AreaSide.OUTSIDE;
        @NotNull final EventStage eventStage = EventStage.END;

        final DriveInsideNotice notice = new DriveInsideNotice(
            messageId,
            fenceId,
            eventId,
            vehicleId,
            dataTime,
            longitude,
            latitude,
            fromArea,
            gotoArea,
            eventStage,
            noticeTime
        );

        final String json = JSON_UTILS.toJson(notice);

        LOG.trace(json);
    }

    @Disabled("打印驶离开始通知")
    @Test
    void printBeginOutsideNoticeJson() {

        @NotNull final AreaSide fromArea = AreaSide.INSIDE;
        @NotNull final AreaSide gotoArea = AreaSide.OUTSIDE;
        @NotNull final EventStage eventStage = EventStage.BEGIN;

        final DriveOutsideNotice notice = new DriveOutsideNotice(
            messageId,
            fenceId,
            eventId,
            vehicleId,
            dataTime,
            longitude,
            latitude,
            fromArea,
            gotoArea,
            eventStage,
            noticeTime
        );

        final String json = JSON_UTILS.toJson(notice);

        LOG.trace(json);
    }

    @Disabled("打印驶离结束通知")
    @Test
    void printEndOutsideNoticeJson() {

        @NotNull final AreaSide fromArea = AreaSide.OUTSIDE;
        @NotNull final AreaSide gotoArea = AreaSide.INSIDE;
        @NotNull final EventStage eventStage = EventStage.END;

        final DriveOutsideNotice notice = new DriveOutsideNotice(
            messageId,
            fenceId,
            eventId,
            vehicleId,
            dataTime,
            longitude,
            latitude,
            fromArea,
            gotoArea,
            eventStage,
            noticeTime
        );

        final String json = JSON_UTILS.toJson(notice);

        LOG.trace(json);
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
    }
}
