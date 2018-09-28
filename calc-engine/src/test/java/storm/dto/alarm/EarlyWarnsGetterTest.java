package storm.dto.alarm;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.util.collect.ImmutableMapUtils;

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

    @DisplayName("测试从数据库导入规则")
    @Test
    void testRebuild() {
        EarlyWarnsGetter.rebuild();

        final ImmutableMap<String, EarlyWarn> rules =
            EarlyWarnsGetter.getRules("ALL");

        Assertions.assertNotNull(rules);
        LOG.warn("rules.size={}", ImmutableMapUtils.filter(rules, (k,v)-> v.level == 0).size());
        Assertions.assertTrue(rules.size() >= 19);
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
