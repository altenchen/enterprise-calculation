package storm.domain.fence;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.domain.fence.area.Coordinate;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: xzj
 */
@DisplayName("电子围栏闭环检测测试")
final class CoordinateCloseCycleTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CoordinateCloseCycleTest.class);

    private CoordinateCloseCycleTest() {
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

    @DisplayName("多边形没有闭环")
    @Test
    public void unCloseCycle() {

        Coordinate c1 = new Coordinate(113.596285, 22.368336);
        Coordinate c2 = new Coordinate(113.600019, 22.368098);
        Coordinate c3 = new Coordinate(113.602079, 22.364884);
        Coordinate c4 = new Coordinate(113.596285, 22.368337);

        List<Coordinate> datas = new ArrayList<>();
        datas.add(c1);
        datas.add(c2);
        datas.add(c3);
        datas.add(c4);

        final Coordinate[] first = {null};
        final Coordinate[] last = {null};
        ImmutableList.Builder<Coordinate> coordinates = new ImmutableList.Builder();
        datas.forEach(item -> {
            if( first[0] == null ){
                first[0] = item;
            }
            last[0] = item;
        });

        Assertions.assertFalse(first[0].equals(last[0]));
    }

    @DisplayName("多边形已闭环")
    @Test
    public void closeCycle() {

        Coordinate c1 = new Coordinate(113.596285, 22.368336);
        Coordinate c2 = new Coordinate(113.600019, 22.368098);
        Coordinate c3 = new Coordinate(113.602079, 22.364884);
        Coordinate c4 = new Coordinate(113.596285, 22.368337);
        Coordinate c5 = new Coordinate(113.596285, 22.368336);

        List<Coordinate> datas = new ArrayList<>();
        datas.add(c1);
        datas.add(c2);
        datas.add(c3);
        datas.add(c4);
        datas.add(c1);
        datas.add(c5);

        final Coordinate[] first = {null};
        final Coordinate[] last = {null};
        ImmutableList.Builder<Coordinate> coordinates = new ImmutableList.Builder();
        datas.forEach(item -> {
            if( first[0] == null ){
                first[0] = item;
            }
            last[0] = item;
        });

        Assertions.assertTrue(first[0].equals(last[0]));
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
