package storm.extension;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * @author: xzp
 * @date: 2018-12-14
 * @description:
 */
@DisplayName("Uuid扩展类测试")
final class UuidExtensionTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(UuidExtensionTest.class);

    private UuidExtensionTest() {
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

    @Disabled
    @DisplayName("测试UUID格式化")
    @RepeatedTest(value = 3, name = "{displayName}: {currentRepetition}/{totalRepetitions}")
    void testToStringWithoutDashes() {

        final UUID uuid = UUID.randomUUID();

        Assertions.assertEquals(
            uuid.toString().replace("-", ""),
            UuidExtension.toStringWithoutDashes(uuid));
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
