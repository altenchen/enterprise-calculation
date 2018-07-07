package storm;

import storm.util.GsonUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author: xzp
 * @date: 2018-07-04
 * @description:
 */
@DisplayName("Gson工具测试")
final class GsonUtilsTest {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(GsonUtilsTest.class);

    private GsonUtilsTest(){}

    private static GsonUtils json;

    @SuppressWarnings("unused")
    @BeforeAll
    private static void beforeAll() {
        json = GsonUtils.getInstance();
    }

    @SuppressWarnings("unused")
    @BeforeEach
    private void beforeEach() {
        // 每个测试之前
    }

    @DisplayName("测试序列化")
    @Test
    void toJson() {
        Map<String, Object> dic = new HashMap<>();
        dic.put("k1", "v1");
        dic.put("k2", "v2");
        dic.put("k3", "v3");
        dic.put("Date", new Date());
        dic.put("NULL", null);
        final String json = GsonUtilsTest.json.toJson(dic);
        logger.trace(json);

        Assertions.assertFalse(StringUtils.isBlank(json), "Json序列化不能为空白");
    }

    @SuppressWarnings("unused")
    @AfterEach
    private void afterEach() {
        // 每个测试之后
    }

    @SuppressWarnings("unused")
    @AfterAll
    private static void afterAll() {
        json = null;
    }
}
