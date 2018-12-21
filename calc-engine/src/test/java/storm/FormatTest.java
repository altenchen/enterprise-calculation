package storm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sun.jersey.core.util.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.*;

/**
 * @author: xzp
 * @date: 2018-07-08
 * @description:
 */
@DisplayName("格式化测试")
final class FormatTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(FormatTest.class);

    private FormatTest() {
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

    @Disabled("时间格式化和反格式化测试")
    @Test
    void timeFormat() throws ParseException {

        final Date now = new Date();
        Date second = DateUtils.setMilliseconds(now, 0);
        LOG.trace("{} -> {}", now, second);

        final TimeZone utcZone = TimeZone.getTimeZone("GMT");
        final TimeZone chinaZone = TimeZone.getTimeZone("Asia/Shanghai");
        final TimeZone chinaZoneToo = TimeZone.getTimeZone("GMT+8:00");

        final Calendar calendar = Calendar.getInstance();
        calendar.set(2018, 7 - 1, 8, 20, 52, 33);
        final Date time = calendar.getTime();
        final long timeInMillis = calendar.getTimeInMillis();
        final String format = "20180708205233";

        final String timeFormat = DateFormatUtils.format(time, FormatConstant.DATE_FORMAT);
        LOG.trace("timeFormat->{}", timeFormat);
        Assertions.assertEquals(format, timeFormat);
        final String timeInMillisFormat = DateFormatUtils.format(timeInMillis, FormatConstant.DATE_FORMAT);
        LOG.trace("timeInMillisFormat->{}", timeInMillisFormat);
        Assertions.assertEquals(format, timeInMillisFormat);

        final Date parseDate = DateUtils.parseDate(format, new String[]{FormatConstant.DATE_FORMAT});
        LOG.trace("parseDate->{}", parseDate);
        Assertions.assertEquals(
            time.getTime() / DateUtils.MILLIS_PER_SECOND,
            parseDate.getTime() / DateUtils.MILLIS_PER_SECOND);

        final Map<String, Map<String, String>> hashMap = new HashMap<>();
        final Map<String, String> key1 = hashMap.getOrDefault("key", new HashMap<>());
        Assertions.assertNotNull(key1);
        final Map<String, String> key2 = hashMap.get("key");
        Assertions.assertNull(key2);
        hashMap.put("key", key1);
        final Map<String, String> key3 = hashMap.get("key");
        Assertions.assertNotNull(key3);
        final Map<String, String> key4 = hashMap.remove("key");
        Assertions.assertNotNull(key4);
        final Map<String, String> key5 = hashMap.remove("key");
        Assertions.assertNull(key5);
    }

    @Disabled("时间范围测试")
    @Test
    void timeRange() {
        try {
            final long tmp_last = 0;
            final long last = DateUtils.parseDate(String.valueOf(tmp_last), new String[]{FormatConstant.DATE_FORMAT}).getTime();
            LOG.trace("last={}", last);
        } catch (ParseException e) {
            return;
        }
        Assertions.fail();
    }

    @Disabled("整数转二进制")
    @Test
    void integerToBinary() {
        final int integer = 0xF0F00F0F;
        final String longBinary = StringUtils.leftPad(Integer.toBinaryString(integer), Long.SIZE, '0');
        Assertions.assertEquals("0000000000000000000000000000000011110000111100000000111100001111", longBinary);
        final String shortBinary = StringUtils.leftPad(Integer.toBinaryString(integer), Short.SIZE, '0');
        Assertions.assertEquals("0000111100001111", shortBinary.substring(shortBinary.length() - Short.SIZE));
    }

    @Disabled("获取完整类名")
    @Test
    void getClassFullName() {
        final Object args = new TreeMap[4];
        final Class<?> type = args.getClass();
        final String name = type.getName();
        LOG.debug("name = {}", name);
        final String canonicalName = type.getCanonicalName();
        LOG.debug("canonicalName = {}", canonicalName);
        final String simpleName = type.getSimpleName();
        LOG.debug("simpleName = {}", simpleName);
        final String typeName = type.getTypeName();
        LOG.debug("typeName = {}", typeName);

        Assertions.assertEquals("org.apache.kafka.common.serialization.StringSerializer", org.apache.kafka.common.serialization.StringSerializer.class.getCanonicalName());
        Assertions.assertEquals("org.apache.kafka.common.serialization.StringSerializer", org.apache.kafka.common.serialization.StringSerializer.class.getCanonicalName());
    }

    @Disabled("不可变类")
    @Test
    void immutableMap() {
        final ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();

        builder.put("key1", "value1");
        builder.put("key2", "value2");
        builder.put("key3", "value3");
        {
            final ImmutableMap<String, String> map1 = builder.build();
            Assertions.assertEquals(map1.get("key1"), "value1");
            Assertions.assertEquals(map1.get("key2"), "value2");
            Assertions.assertEquals(map1.get("key3"), "value3");

            builder.put("key9", "value9");
            builder.put("key8", "value8");
            builder.put("key7", "value7");

            Assertions.assertNull(map1.get("key9"));
            Assertions.assertNull(map1.get("key8"));
            Assertions.assertNull(map1.get("key7"));
        }

        {
            final ImmutableMap<String, String> map2 = builder.build();
            Assertions.assertEquals(map2.get("key1"), "value1");
            Assertions.assertEquals(map2.get("key2"), "value2");
            Assertions.assertEquals(map2.get("key3"), "value3");
            Assertions.assertEquals(map2.get("key9"), "value9");
            Assertions.assertEquals(map2.get("key8"), "value8");
            Assertions.assertEquals(map2.get("key7"), "value7");
        }

        final HashMap<String, String> hashMap = Maps.newHashMap();
        hashMap.put("key9", "value99");
        hashMap.put("key8", "value88");
        hashMap.put("key7", "value77");
        Assertions.assertEquals(hashMap.get("key9"), "value99");
        Assertions.assertEquals(hashMap.get("key8"), "value88");
        Assertions.assertEquals(hashMap.get("key7"), "value77");

        hashMap.putAll(builder.build());
        Assertions.assertEquals(hashMap.get("key9"), "value9");
        Assertions.assertEquals(hashMap.get("key8"), "value8");
        Assertions.assertEquals(hashMap.get("key7"), "value7");


        builder.put("key9", "value99");
        builder.put("key8", "value88");
        builder.put("key7", "value77");
        try {
            builder.build();
            Assertions.fail();
        } catch (Exception e) {
        }

    }

    @Disabled("测试更新Map")
    @Test
    void computeMap() {

        final String vid = "vid";
        final String status = "status";

        final Map<String, Map<String, String>> cacheMap = Maps.newHashMap();
        Assertions.assertTrue(cacheMap.isEmpty());
        final Map<String, String> initCache = cacheMap.get(vid);
        Assertions.assertNull(initCache);

        {
            final String create = "create";
            final Map<String, String> createMap = Maps.newHashMap();
            createMap.put(status, create);
            cacheMap.compute(vid, (key, oldValue) -> {
                final Map<String, String> newValue = null == oldValue ? Maps.newConcurrentMap() : oldValue;
                newValue.putAll(createMap);
                return newValue;
            });
            Assertions.assertTrue(cacheMap.containsKey(vid));
            final Map<String, String> createCache = cacheMap.get(vid);
            Assertions.assertNotNull(createCache);
            Assertions.assertEquals(create, createCache.get(status));
        }

        {
            final String update = "update";
            final Map<String, String> updateMap = Maps.newHashMap();
            updateMap.put(status, update);
            cacheMap.compute(vid, (key, oldValue) -> {
                final Map<String, String> newValue = null == oldValue ? Maps.newConcurrentMap() : oldValue;
                newValue.putAll(updateMap);
                return newValue;
            });
            Assertions.assertTrue(cacheMap.containsKey(vid));
            final Map<String, String> updateCache = cacheMap.get(vid);
            Assertions.assertNotNull(updateCache);
            Assertions.assertEquals(update, updateCache.get(status));
        }

    }

    @Disabled("打印可用的字符集")
    @Test
    void printAvailableCharsets() {
        final SortedMap<String, Charset> availableCharsets = Charset.availableCharsets();
        availableCharsets.forEach((s, charset) -> {
            LOG.debug("[{}] -> [{}]", s, charset);
        });
    }

    @Disabled("解析7003数据")
    @Test
    void parse7003() throws UnsupportedEncodingException {
        final String source = "DKsMpgy0DKMMqQysDKEMoAylDKsMoQylDJ4MpQyoDJ0MpAymDJQMoQyYDJcMmAykDKgMugypDJ8MpwybDJ8MqQyjDJgMoAylDKcMpgygDKYMowyoDLAMqAysDJ8MsAysDKIMpgyXDKkMkAywDJEMqgylDMQMpgygDKIMqgy/DKgMrQysDKkMlgyrDKMMkQyuDJkMrgyoDK0MmQyoDJUMrQylDKsMoQymDJQMpAyhDJ0MoAyuDKsMqQycDK0MqQyQDLEMqwynDK0MpwyiDKQMqA==";
        final byte[] base64 = Base64.decode(source);
        LOG.debug("单体电池电压数={}", base64.length / 2);
        final ByteArrayInputStream byteArrayStream = new ByteArrayInputStream(base64);
        final DataInputStream dataStream = new DataInputStream(byteArrayStream);
        try {
            if (dataStream.available() % 2 != 0) {
                LOG.warn("[7003]数据不完整.");
            }
            while (dataStream.available() >= 2) {
                final int unsignedShort = dataStream.readUnsignedShort();
                LOG.debug("单体电池电压值={}", unsignedShort);
            }
        } catch (IOException e) {
            LOG.warn("[7003]解析异常.", e);
        }
    }

    @Disabled("解析7103数据")
    @Test
    void parse7103() throws UnsupportedEncodingException {
        final String source = "SklKSkhHSEdHSEhISA==";
        final byte[] base64 = Base64.decode(source);
        LOG.debug("单体电池温度数={}", base64.length);
        final ByteArrayInputStream byteArrayStream = new ByteArrayInputStream(base64);
        final DataInputStream dataStream = new DataInputStream(byteArrayStream);
        try {
            while (dataStream.available() >= 1) {
                final int unsignedByte = dataStream.readUnsignedByte();
                LOG.debug("单体电池温度值={}", unsignedByte);
            }
        } catch (IOException e) {
            LOG.warn("[7103]解析异常.", e);
        }
    }

    @Disabled("测试序列化机制")
    @Test
    void serial() {

        try {

            final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);

            final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

            final Person sourcePerson = new Person();
            sourcePerson.name = "xzp";
            sourcePerson.age = 31;
            sourcePerson.transientString = "nat";

            LOG.trace("sourcePerson={}", sourcePerson);

            objectOutputStream.writeObject(sourcePerson);

            final byte[] objectArray = byteArrayOutputStream.toByteArray();

            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(objectArray);

            final ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);

            final Person targetPerson = (Person) objectInputStream.readObject();

            LOG.trace("targetPerson={}", targetPerson);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Disabled("验证装箱拆箱")
    @Test
    void boxDouble() {
        final Double doubleObj = null;
        Assertions.assertNull(doubleObj);

        try {
            final double doubleValue = doubleObj;
            LOG.trace("doubleValue=[{}]", doubleValue);
            Assertions.fail();
        } catch (NullPointerException e) {
            LOG.trace("doubleObj=[{}]", doubleObj);
        }

        final double coefficient = 0;
        LOG.trace("coefficient = {}", coefficient);
        Assertions.assertTrue(-Double.MIN_NORMAL < coefficient);
        Assertions.assertTrue(coefficient < Double.MIN_NORMAL);
        Assertions.assertEquals(0d, 0f);

        final double zero = -1 / 0d;
        LOG.trace("1d * 0 = {}", zero);
        Assertions.assertTrue(Double.isInfinite(zero));
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, zero);
    }

    private static class Person implements Serializable {

        private static final long serialVersionUID = -7322732111703395160L;

        public String name;

        public Integer age;

        public final String finalString = UUID.randomUUID().toString();

        public transient String transientString = "TransientString";

        public transient final String transientFinalString = UUID.randomUUID().toString();

        {
            LOG.trace("对象初始化块");
        }

        public Person() {
            LOG.trace("默认构造函数");
        }

        @Override
        public String toString() {
            return new StringBuilder()
                .append("[name=")
                .append(name)
                .append(",age=")
                .append(age)
                .append(",finalString=")
                .append(finalString)
                .append(",transientString=")
                .append(transientString)
                .append(",transientFinalString=")
                .append(transientFinalString)
                .append("]")
                .toString();
        }
    }

    @Disabled("测试流式函数")
    @Test
    void testStream() {
        final ImmutableSet<String> set = ImmutableSet.of("xx", "yy", "zz");

        Assertions.assertEquals(true, set.parallelStream().noneMatch(s -> StringUtils.equals(s, "aa")));
        Assertions.assertEquals(true, set.parallelStream().noneMatch(s -> StringUtils.equals(s, "bb")));
        Assertions.assertEquals(true, set.parallelStream().noneMatch(s -> StringUtils.equals(s, "cc")));
        Assertions.assertEquals(false, set.parallelStream().noneMatch(s -> StringUtils.equals(s, "xx")));
        Assertions.assertEquals(false, set.parallelStream().noneMatch(s -> StringUtils.equals(s, "yy")));
        Assertions.assertEquals(false, set.parallelStream().noneMatch(s -> StringUtils.equals(s, "zz")));

        Assertions.assertEquals(false, set.parallelStream().anyMatch(s -> StringUtils.equals(s, "aa")));
        Assertions.assertEquals(false, set.parallelStream().anyMatch(s -> StringUtils.equals(s, "bb")));
        Assertions.assertEquals(false, set.parallelStream().anyMatch(s -> StringUtils.equals(s, "cc")));
        Assertions.assertEquals(true, set.parallelStream().anyMatch(s -> StringUtils.equals(s, "xx")));
        Assertions.assertEquals(true, set.parallelStream().anyMatch(s -> StringUtils.equals(s, "yy")));
        Assertions.assertEquals(true, set.parallelStream().anyMatch(s -> StringUtils.equals(s, "zz")));
    }

    @Disabled("验证BigDecimal特性")
    @Test
    void testBigDecimal() {
        final BigDecimal _1_2345 = NumberUtils.createBigDecimal("1.2345");
        Assertions.assertEquals(4, _1_2345.scale());

        final BigDecimal _123_45 = NumberUtils.createBigDecimal("123.45");
        Assertions.assertEquals(2, _123_45.scale());

        final BigDecimal _123_450 = NumberUtils.createBigDecimal("123.450");
        Assertions.assertEquals(3, _123_450.scale());

        // 值相同 且 精度相同 的小数才相同
        Assertions.assertNotEquals(_123_45, _123_450);
        Assertions.assertEquals(_123_45, _123_450.stripTrailingZeros());

        // 只比较值大小的话, 应当使用 compareTo 方法, 小于返回 -1, 等于返回0, 大于返回 1
        Assertions.assertTrue(_1_2345.compareTo(_123_45) < 0);
        Assertions.assertTrue(_1_2345.compareTo(_123_45) == -1);
        Assertions.assertTrue(_123_45.compareTo(_1_2345) > 0);
        Assertions.assertTrue(_123_45.compareTo(_1_2345) == 1);
        Assertions.assertTrue(_123_45.compareTo(_123_450) == 0);

        final BigDecimal _1_2345_multiply_123_45 = _1_2345.multiply(_123_450).stripTrailingZeros();
        Assertions.assertEquals(6, _1_2345_multiply_123_45.scale());
        Assertions.assertEquals(new BigDecimal("152.399025"), _1_2345_multiply_123_45);

        final BigDecimal _0_01 = NumberUtils.createBigDecimal("0.01");
        final BigDecimal _1_2345_divide_0_01 = _1_2345.divide(_0_01).stripTrailingZeros();
        Assertions.assertEquals(2, _1_2345_divide_0_01.scale());
        Assertions.assertEquals(_123_450.stripTrailingZeros(), _1_2345_divide_0_01);

        final BigDecimal _2 = NumberUtils.createBigDecimal("2");
        final BigDecimal _123_45_multiply_2 = _123_450.multiply(_2).stripTrailingZeros();
        Assertions.assertEquals(1, _123_45_multiply_2.scale());

        final BigDecimal _123_45_divide_2 = _123_450.divide(_2);
        Assertions.assertEquals(3, _123_45_divide_2.scale());

        final BigDecimal _7 = NumberUtils.createBigDecimal("7.0");
        final BigDecimal _123_45_divide_7 = _123_450.divide(_7, RoundingMode.HALF_UP).stripTrailingZeros();
        Assertions.assertEquals(_123_450.scale()-_7.stripTrailingZeros().scale(), _123_45_divide_7.scale());
        Assertions.assertEquals(new BigDecimal("17.636"), _123_45_divide_7);

        try {
            _123_450.divide(_7);
        } catch (final ArithmeticException ignored) {

        }

        final BigDecimal speed = new BigDecimal("120");
        final BigDecimal soc = new BigDecimal("70");
        final BigDecimal result = new BigDecimal("2.0");
        final BigDecimal divide = speed.divide(soc, Math.max(result.scale(), Math.max(speed.scale(), soc.scale())), BigDecimal.ROUND_HALF_UP);
        Assertions.assertEquals(new BigDecimal("1.7"), divide);
    }

    @Disabled("验证BigDecimal特性")
    @Test
    void testHashSetOnString() {
        final String first = "123";
        final String second = "123";
        final String third = new String("123");

        Assertions.assertSame(first, second);
        Assertions.assertNotSame(first, third);
        Assertions.assertNotSame(second, third);

        Assertions.assertEquals(first, second);
        Assertions.assertEquals(first, third);
        Assertions.assertEquals(second, third);

        Assertions.assertEquals(first.hashCode(), second.hashCode());
        Assertions.assertEquals(first.hashCode(), third.hashCode());
        Assertions.assertEquals(second.hashCode(), third.hashCode());

        final HashSet<String> hashSet = Sets.newHashSet();

        hashSet.add(first);
        hashSet.remove(second);
        Assertions.assertTrue(hashSet.isEmpty());

        hashSet.add(first);
        hashSet.remove(third);
        Assertions.assertTrue(hashSet.isEmpty());

        hashSet.add(second);
        hashSet.remove(third);
        Assertions.assertTrue(hashSet.isEmpty());
    }

    @Disabled("验证ImmutableSet特性")
    @Test
    void testImmutableSet() {
        final ImmutableSet<String> _111 = ImmutableSet.of("1", "1", "1");
        Assertions.assertEquals(1, _111.size());

        final ImmutableSet<String> _1111 = ImmutableSet.copyOf(new String[]{"1", "1", "1", "1"});
        Assertions.assertEquals(1, _1111.size());

        final ImmutableSet<String> _11 = new ImmutableSet.Builder<String>().add("1").add("1").build();
        Assertions.assertEquals(1, _11.size());
    }

    @Disabled("验证Map值类型向基类转换")
    @Test
    void testMapConvert() {
        {
            final TreeMap<String, String> source = Maps.newTreeMap();
            source.put("一二三四五", "上山打老虎");

            final TreeMap<String, Object> destination = Maps.newTreeMap(source);
            destination.forEach((k, v) -> LOG.trace("{} -> {}", k, v));

            destination.put("六七八九十", 67890);
            source.forEach((k, v) -> LOG.trace("{} -> {}", k, v));
        }
        {
            final HashMap<String, String> source = Maps.newHashMap();
            source.put("锄禾日当午", "汗滴禾下土");

            final HashMap<String, Object> destination = Maps.newHashMap(source);
            destination.forEach((k, v) -> LOG.trace("{} -> {}", k, v));

            destination.put("谁知盘中餐", "粒粒皆辛苦");
            source.forEach((k, v) -> LOG.trace("{} -> {}", k, v));
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
