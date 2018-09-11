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

    @DisplayName("测试偏移系数")
    @Test
    void testMethod() {
        final ImmutableMap<String, CoefficientOffset> coefficientOffsets = CoefficientOffsetGetter.getCoefficientOffsets();
        coefficientOffsets.forEach((dataKey, coefficientOffset) -> {
            if(StringUtils.isBlank(dataKey)) {
                final FormattingTuple tuple = MessageFormatter.arrayFormat(
                    "dataKey不能为空[{}][{}]",
                    new Object[]{
                        dataKey,
                        coefficientOffset
                    }
                );
                Assertions.fail(tuple.getMessage());
            }
            if(!dataKey.equals(coefficientOffset.getDataKey())) {
                final FormattingTuple tuple = MessageFormatter.arrayFormat(
                    "key[{}]与dataKey[{}]必须相等",
                    new Object[]{
                        dataKey,
                        coefficientOffset.getDataKey()
                    }
                );
                Assertions.fail(tuple.getMessage());
            }
        });
    }

    @DisplayName("测试偏移系数配置文件")
    @Test
    void initFromResource() {

        final String resourceName = "coefficient_offset.json";
        final InputStream stream = ConfigUtils.class.getClassLoader().getResourceAsStream(resourceName);
        if(null == stream) {
            final FormattingTuple tuple = MessageFormatter.arrayFormat(
                "初始化偏移系数的资源文件[{}]不存在",
                new Object[]{
                    resourceName
                }
            );
            final String message = tuple.getMessage();
            Assertions.fail(message);
            return;
        }

        final ArrayList<CoefficientOffset> coefficientOffsets;
        try {
            coefficientOffsets =  JSON.parseObject(
                stream,
                Charset.forName("UTF-8"),
                new TypeToken<ArrayList<CoefficientOffset>>() {
                }.getType()
            );
        } catch (@NotNull final Exception e) {
            final FormattingTuple tuple = MessageFormatter.arrayFormat(
                "从资源文件初始化偏移系数异常[{}]",
                new Object[]{
                    e.getMessage()
                }
            );
            final String message = tuple.getMessage();
            Assertions.fail(message);
            return;
        }

        if(CollectionUtils.isNotEmpty(coefficientOffsets)) {
            final HashMap<String, CoefficientOffset> coefficientOffsetMap = Maps.newHashMapWithExpectedSize(coefficientOffsets.size());
            coefficientOffsets.forEach(coefficientOffset -> {
                final String key = coefficientOffset.getDataKey();
                if (coefficientOffsetMap.containsKey(key)) {
                    final FormattingTuple tuple = MessageFormatter.arrayFormat(
                        "冲突的偏移系数配置[{}]",
                        new Object[]{
                            key
                        }
                    );
                    final String message = tuple.getMessage();
                    Assertions.fail(message);
                    return;
                }
                coefficientOffsetMap.put(key, coefficientOffset);
            });
        } else {
            Assertions.fail("偏移系数规则为空");
        }
    }

    @Disabled("偏移系数序列化")
    @Test
    void coefficientOffsetJson() throws IOException {
        final ImmutableSortedSet<CoefficientOffset> coefficientOffsets = ImmutableSortedSet.copyOf(
            (c1, c2) -> c2.getDataKey().compareTo(c1.getDataKey()),
            CoefficientOffsetGetter.getCoefficientOffsets().values());

        final JsonUtils jsonUtils = JsonUtils.getInstance();

        final String coefficientOffsetsJson = jsonUtils.toJson(coefficientOffsets);

        LOG.debug("coefficientOffsetsJson={}", coefficientOffsetsJson);
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
