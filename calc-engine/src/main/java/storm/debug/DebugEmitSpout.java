package storm.debug;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.Utils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.extension.UuidExtension;
import storm.protocol.CommandType;
import storm.stream.GeneralStream;
import storm.stream.MessageId;
import storm.stream.StreamReceiverFilter;
import storm.system.DataKey;
import storm.util.DataUtils;
import storm.util.JedisPoolUtils;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author: xzp
 * @date: 2018-12-13
 * @description:
 */
public final class DebugEmitSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(DebugEmitSpout.class);

    private static final JedisPoolUtils JEDIS_POOL_UTILS =
        JedisPoolUtils.getInstance();

    // region Component

    @NotNull
    private static final String COMPONENT_ID = DebugEmitSpout.class.getSimpleName();

    @SuppressWarnings("unused")
    @NotNull
    @Contract(pure = true)
    public static String getComponentId() {
        return COMPONENT_ID;
    }

    // endregion Component

    // region GeneralStream

    @NotNull
    private static final GeneralStream STORM_STREAM = GeneralStream.getInstance();

    @NotNull
    private static final String GENERAL_STREAM_ID = STORM_STREAM.getStreamId(COMPONENT_ID);

    private static final ImmutableMap<String, String> DEFAULT_DATA =
        ImmutableMap.<String, String>builder()
            .put(DataKey.PREFIX, CommandType.SUBMIT)
            .put(DataKey.SERIAL_NO, "0")
            .put(DataKey.VEHICLE_NUMBER, UuidExtension.toStringWithoutDashes(UUID.randomUUID()).toUpperCase())
            .put(DataKey.MESSAGE_TYPE, CommandType.SUBMIT_REALTIME)
            .put(DataKey.VEHICLE_ID, UUID.randomUUID().toString())
            .put(DataKey.CAR_TYPE, "1_1_1")
            .put(DataKey.VEHICLE_TYPE, UuidExtension.toStringWithoutDashes(UUID.randomUUID()).toLowerCase())
            // ↑固定数据项↑
            // ↓动态数据项↓
            .put(DataKey._2000_TERMINAL_COLLECT_TIME, DataUtils.buildFormatTime(System.currentTimeMillis()))
            .put(DataKey._2001, "94")
            .put(DataKey._2002, "1")
            .put(DataKey._2101, "40")
            .put(DataKey._2102, "1")
            .put(DataKey._2103_SINGLE_TEMP, "MTo1M181M181M181NF81M181NF81M181M181M181M181M181M181M181NF81NF81NF81NF81M181M181M181Ml81M181M181M181M181M181M181NF81NF81NF81NF81NF81NF81NF81M181M181M181NF81M181Mw==")
            .put(DataKey._2201_SPEED, "0")
            .put(DataKey._2202_TOTAL_MILEAGE, "110170")
            .put(DataKey._2203_GEARS, "0")
            .put(DataKey._2204_BRAKING_FORCE, "0")
            .put(DataKey._2205_DRIVING_FORCE, "0")
            .put(DataKey._2208_ACCELERATOR_PEDAL, "0")
            .put(DataKey._2209_BRAKING_PEDAL, "0")
            .put(DataKey._2213_RUNNING_MODE, "1")
            .put(DataKey._2214, "1")
            .put(DataKey._2301_CHARGE_STATUS, "1")
            .put(DataKey._2307_DRIVING_ELE_MAC_COUNT, "1")
            .put(DataKey._2308_DRIVING_ELE_MAC_LIST, "MjMwOToxLDIzMTA6LDIzMDI6LDIzMDM6LDIzMTE6LDIzMDQ6LDIzMDU6LDIzMDY6")
            .put(DataKey._2501_ORIENTATION, "0")
            .put(DataKey._2502_LONGITUDE, "103734629")
            .put(DataKey._2503_LATITUDE, "30524003")
            .put(DataKey._2601_HIGHVOLT_CHILD_NUM, "1")
            .put(DataKey._2602_HIGHVOLT_SINGLE_NUM, "3")
            .put(DataKey._2603_SINGLE_VOLT_HIGN_VAL, "3994")
            .put(DataKey._2604_LOWVOLT_CHILD_NUM, "1")
            .put(DataKey._2605_LOWVOLT_SINGLE_NUM, "34")
            .put(DataKey._2606_SINGLE_VOLT_LOW_VAL, "3984")
            .put(DataKey._2607_HIGNTEMP_CHILD, "1")
            .put(DataKey._2608_SINGLE_HIGNTEMP_NUM, "4")
            .put(DataKey._2609_SINGLE_HIGNTEMP_VAL, "54")
            .put(DataKey._2610_LOWTEMP_CHILD, "1")
            .put(DataKey._2611_SINGLE_LOWTEMP_NUM, "21")
            .put(DataKey._2612_SINGLE_LOWTEMP_VAL, "52")
            .put(DataKey._2613_TOTAL_VOLTAGE, "3748")
            .put(DataKey._2614_TOTAL_ELECTRICITY, "9840")
            .put(DataKey._2615_STATE_OF_CHARGE_BEI_JIN, "76")
            .put(DataKey._2617_INSULATION_RESISTANCE, "2999")
            .put(DataKey._2804, "0")
            .put(DataKey._2805, "")
            .put(DataKey._2808, "0")
            .put(DataKey._2809, "")
            .put(DataKey._2920_ALARM_STATUS, "0")
            .put(DataKey._2921, "0")
            .put(DataKey._2922, "")
            .put(DataKey._2923, "0")
            .put(DataKey._2924, "")
            .put(DataKey._3201_CAR_STATUS, "2")
            .put(DataKey._3801_ALARM_MARK, "0")
            .put(DataKey._7001, "94")
            .put(DataKey._7002, "1")
            .put(DataKey._7003_SINGLE_VOLT_ORIG, "D5QPlQ+aD5IPkw+VD5QPkg+WD5UPkg+UD5EPkw+SD5IPkg+RD5QPlQ+RD5IPlQ+WD5UPlg+UD5UPlQ+UD5QPlg+WD5APlA+UD5UPlA+VD5QPkw+SD5UPlQ+YD5gPlw+WD5UPkw+UD5UPlg+VD5YPkQ+TD5UPlA+VD5QPlA+UD5MPlA+VD5UPlA+RD5UPlQ+UD5QPkA+WD5IPkA+RD5UPkg+WD5EPlQ+VD5IPlQ+XD5IPkg+SD5UPkQ+UD5U=")
            .put(DataKey._7101, "40")
            .put(DataKey._7102, "1")
            .put(DataKey._7103_SINGLE_TEMP_ORGI, "NTU1NjU2NTU1NTU1NTY2NjY1NTU0NTU1NTU1NjY2NjY2NjU1NTY1NQ==")
            .put(DataKey._7615_STATE_OF_CHARGE, "76")
            .put(DataKey._9999_PLATFORM_RECEIVE_TIME, DataUtils.buildFormatTime(System.currentTimeMillis()))
            .build();

    @SuppressWarnings("unused")
    @NotNull
    @Contract(pure = true)
    public static String getGeneralStreamId() {
        return GENERAL_STREAM_ID;
    }

    @SuppressWarnings("unused")
    @NotNull
    public static StreamReceiverFilter prepareGeneralStreamReceiver(
        @NotNull final GeneralStream.IProcessor processor) {

        return STORM_STREAM
            .prepareReceiver(
                processor)
            .filter(
                COMPONENT_ID,
                GENERAL_STREAM_ID);
    }

    // endregion GeneralStream

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private transient SpoutOutputCollector collector;

    private transient GeneralStream.SpoutSender generalStreamSender;

    @Override
    public void open(
        @NotNull final Map conf,
        @NotNull final TopologyContext context,
        @NotNull final SpoutOutputCollector collector) {

        this.collector = collector;

        generalStreamSender = STORM_STREAM.prepareSpoutSender(GENERAL_STREAM_ID, collector);
    }

    @Override
    public void nextTuple() {
        final String currentTimeFormatString = DataUtils.buildFormatTime(
            System.currentTimeMillis()
        );
        try {
            final String redisKey = "storm.debug.vehicle";
            JEDIS_POOL_UTILS.useResource(jedis -> {
                jedis.select(0);
                final Map<String, String> data = jedis.hgetAll(redisKey);
                DEFAULT_DATA.forEach((field, value) -> {
                    data.computeIfAbsent(field, key -> {
                        jedis.hset(redisKey, field, value);
                        return value;
                    });
                });
                data.compute(
                    DataKey.SERIAL_NO,
                    (field, value) -> String.valueOf(
                        NumberUtils.toLong(value, 0L) + 1L)
                );
                data.put(
                    DataKey._2000_TERMINAL_COLLECT_TIME,
                    currentTimeFormatString
                );
                data.put(
                    DataKey._9999_PLATFORM_RECEIVE_TIME,
                    currentTimeFormatString
                );

                final String prefix = data.get(DataKey.PREFIX);
                final String serialNo = data.get(DataKey.SERIAL_NO);
                final String vin = data.get(DataKey.VEHICLE_NUMBER);
                final String cmd = data.get(DataKey.MESSAGE_TYPE);
                final String vehicleId = data.get(DataKey.VEHICLE_ID);

                jedis.hset(
                    redisKey,
                    DataKey.SERIAL_NO,
                    serialNo
                );
                jedis.hset(
                    redisKey,
                    DataKey._2000_TERMINAL_COLLECT_TIME,
                    currentTimeFormatString
                );
                jedis.hset(
                    redisKey,
                    DataKey._9999_PLATFORM_RECEIVE_TIME,
                    currentTimeFormatString
                );

                final StringBuilder builder = new StringBuilder();
                builder.append(prefix).append(" ");
                builder.append(serialNo).append(" ");
                builder.append(vin).append(" ");
                builder.append(cmd).append(" {");
                builder.append(DataKey.VEHICLE_ID).append(':').append(data.get(DataKey.VEHICLE_ID)).append(',');

                data.forEach((key, value) -> {
                    if( key.equals(DataKey.VEHICLE_ID) ){
                        return;
                    }
                    builder.append(',').append(key).append(':').append(value);
                });
                builder.append("}");
                final String content = builder.toString();

                generalStreamSender.emit(new MessageId<>(vehicleId), vehicleId, content);
            });
        } catch (@NotNull final Exception e) {
            LOG.error("", e);
        } finally {
            Utils.sleep(TimeUnit.SECONDS.toMillis(10));
        }
    }

    @Override
    public void declareOutputFields(
        @NotNull final OutputFieldsDeclarer declarer) {

        STORM_STREAM.declareOutputFields(GENERAL_STREAM_ID, declarer);
    }
}
