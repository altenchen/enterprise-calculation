package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.system.SysDefine;
import storm.util.ConfigUtils;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author: xzp
 * @date: 2018-09-28
 * @description:
 */
@DisplayName("EarlyWarnsGetterTest")
public final class EarlyWarnsGetterTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(EarlyWarnsGetterTest.class);

    private EarlyWarnsGetterTest() {
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

    @Disabled("测试从数据库导入规则")
    @Test
    void testRebuild() {

//        ConfigUtils.getInstance().sysDefine
//            .setProperty(SysDefine.DB_CACHE_FLUSH_TIME_SECOND, "3");

        Runnable test = ()->{
            final ImmutableMap<String, EarlyWarn> rules =
                EarlyWarnsGetter.getRulesByVehicleModel("ALL");

            Assertions.assertNotNull(rules);
            Assertions.assertTrue(rules.size() >= 19);
        };

        for (int i = 0; i < 100; i++) {
            test.run();
        }

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            LOG.error("休眠异常", e);
        }

        for (int i = 0; i < 100; i++) {
            test.run();
        }
    }

    @Disabled("获取所有可用规则ID")
    @Test
    void getEnableRuleIds() {
        ImmutableSet.copyOf(
            EarlyWarnsGetter.getAllRules()
                .values()
                .stream()
                .flatMap(rules -> rules.keySet().stream())
                .peek(LOG::trace)
                .collect(Collectors.toSet())
        );
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
