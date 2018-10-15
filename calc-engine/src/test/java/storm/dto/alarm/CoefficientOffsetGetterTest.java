package storm.dto.alarm;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.*;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;
import storm.util.ConfigUtils;
import storm.util.JsonUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
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
                        LOG.debug("M[{}]D[{}]I[{}]", vehicleModel, dataKey, coefficientOffset.itemId);
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
