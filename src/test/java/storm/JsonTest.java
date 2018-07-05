package storm;

import storm.util.IJson;
import storm.util.JsonUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-07-04
 * @description:
 */
@DisplayName("Json工具测试")
public class JsonTest {

    private static Logger logger;

    private static IJson json;

    @BeforeAll
    public static void beforeAll() {
        logger = LoggerFactory.getLogger(JsonTest.class);
        json = JsonUtils.getInstance();
    }

    @BeforeEach
    public void beforeEach() {
        // 每个测试之前
    }

    @Test
    public void testToJson() {
        Map<String, String> dic = new HashMap<>();
        dic.put("k1", "v1");
        dic.put("k2", "v2");
        dic.put("k3", "v3");
        final String json = JsonTest.json.toJson(dic);
        logger.info(json);

        Assertions.assertFalse(StringUtils.isBlank(json), "Json序列化不能为空白");
    }

    @AfterEach
    public void afterEach() {
        // 每个测试之后
    }

    @AfterAll
    public static void afterAll() {
        json = null;
        logger = null;
    }
}
