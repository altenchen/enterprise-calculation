package storm.dto.alarm;

import com.google.common.collect.*;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author: xzp
 * @date: 2018-09-13
 * @description:
 */
@DisplayName("CoefficientOffsetGetterTest")
public final class CoefficientOffsetGetterTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CoefficientOffsetGetterTest.class);

    private CoefficientOffsetGetterTest() {
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

    @Disabled("测试偏移系数")
    @Test
    void testMethod() {
        final ImmutableMap<String, ImmutableMap<String, CoefficientOffset>> coefficientOffsets =
            CoefficientOffsetGetter.getAllCoefficientOffsets();
        LOG.debug("count(model)={}", coefficientOffsets.size());
        coefficientOffsets.forEach(
            (vehicleModel, dataCoefficientOffset) -> {
                LOG.debug("[{}]count(key)={}", vehicleModel, dataCoefficientOffset.size());
                dataCoefficientOffset.forEach(
                    (dataKey, coefficientOffset) -> {
                        LOG.debug("M[{}]D[{}]I[{}]", vehicleModel, dataKey, coefficientOffset.getItemId());
                    }
                );
            }
        );
        LOG.debug("total={}", coefficientOffsets.values().stream().map(Map::size).reduce(0, (l, r) -> l + r));
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
