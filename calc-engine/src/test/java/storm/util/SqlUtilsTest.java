package storm.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * @author: xzp
 * @date: 2018-09-19
 * @description:
 */
@DisplayName("SqlUtilsTest")
public final class SqlUtilsTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(SqlUtilsTest.class);

    private SqlUtilsTest() {
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

    @DisplayName("测试单例")
    @Test
    void getInstance() {

        final SqlUtils first = SqlUtils.getInstance();
        final SqlUtils second = SqlUtils.getInstance();
        Assertions.assertSame(first, second);
    }

    @DisplayName("测试查询")
    @Test
    void query() {
        final SqlUtils sqlUtils = SqlUtils.getInstance();
        final JsonUtils jsonUtils = JsonUtils.getInstance();
        final ImmutableList<String> resultJsonList = sqlUtils.query(
            "SELECT veh.uuid vid, veh.vin vin, model.id mid FROM sys_vehicle veh LEFT JOIN sys_veh_model model ON veh.veh_model_id=model.id",
            (resultSet -> {
                final ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
                while (resultSet.next()) {
                    final ResultSetMetaData metaData = resultSet.getMetaData();
                    final TreeMap<Integer, Object> row = Maps.newTreeMap();
                    for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); ++columnIndex) {
                        row.put(columnIndex, resultSet.getObject(columnIndex));
                    }
                    final String json = jsonUtils.toJson(row);
                    builder.add(json);
                }
                return builder.build();
            }));
        if (resultJsonList == null) {
            Assertions.fail();
        }
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
