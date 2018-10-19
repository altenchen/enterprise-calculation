package storm;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import storm.util.ConfigUtils;
import storm.util.JsonUtils;

import java.io.*;

/**
 * @author: xzp
 * @date: 2018-09-12
 * @description:
 */
@DisplayName("SnakeYamlTest")
public final class SnakeYamlTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(SnakeYamlTest.class);

    private SnakeYamlTest() {
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
