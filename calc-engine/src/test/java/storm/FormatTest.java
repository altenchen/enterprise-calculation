package storm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.sun.jersey.core.util.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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

        final TimeZone utcZone = TimeZone.getTimeZone("GMT");
        final TimeZone chinaZone = TimeZone.getTimeZone("Asia/Shanghai");
        final TimeZone chinaZoneToo = TimeZone.getTimeZone("GMT+8:00");

        final Calendar calendar = Calendar.getInstance();
        calendar.set(2018, 7-1, 8, 20, 52, 33);
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
            if(dataStream.available() % 2 != 0) {
                LOG.warn("[7003]数据不完整.");
            }
            while(dataStream.available() >= 2) {
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
            while(dataStream.available() >= 1) {
                final int unsignedByte = dataStream.readUnsignedByte();
                LOG.debug("单体电池温度值={}", unsignedByte);
            }
        } catch (IOException e) {
            LOG.warn("[7103]解析异常.", e);
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
