package storm.dto.alarm;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.system.DataKey;
import storm.util.dbconn.Conn;

/**
 * @author wza
 * 偏移系数自定义数据项处理
 */
public final class CoefficientOffsetGetter {

    @NotNull
    private static final Logger LOG = LoggerFactory.getLogger(CoefficientOffsetGetter.class);

    @NotNull
    private static final ImmutableMap<String, CoefficientOffset> FIXED_COEFFICIENT_OFFSETS =
        // region 初始化固定偏移系数
        new ImmutableMap.Builder<String, CoefficientOffset>()
            // 电机控制器温度
            .put(DataKey._2302_DRIVING_ELE_MAC_TEMPCTOL, new CoefficientOffset(DataKey._2302_DRIVING_ELE_MAC_TEMPCTOL, 0, 1, 40))
            // 驱动电机温度
            .put(DataKey._2304_DRIVING_ELE_MAC_TEMP, new CoefficientOffset(DataKey._2304_DRIVING_ELE_MAC_TEMP, 0, 1, 40))
            // ECU温度
            .put(DataKey._2402, new CoefficientOffset(DataKey._2402, 0, 1, 40))
            // 发动机温度
            .put(DataKey._2404, new CoefficientOffset(DataKey._2404, 0, 1, 40))
            // 进气温度
            .put(DataKey._2406, new CoefficientOffset(DataKey._2406, 0, 1, 40))
            // 废气排出温度
            .put(DataKey._2407, new CoefficientOffset(DataKey._2407, 0, 1, 40))
            // 电池单体最高温度值
            .put(DataKey._2609_SINGLE_HIGNTEMP_VAL, new CoefficientOffset(DataKey._2609_SINGLE_HIGNTEMP_VAL, 0, 1, 40))
            // 电池单体最低温度值
            .put(DataKey._2612_SINGLE_LOWTEMP_VAL, new CoefficientOffset(DataKey._2612_SINGLE_LOWTEMP_VAL, 0, 1, 40))
            // 单体温度值列表
            .put(DataKey._2103_SINGLE_TEMP, new CoefficientOffset(DataKey._2103_SINGLE_TEMP, 0, 1, 40))
            // 驱动电机母线电流
            .put(DataKey._2306_DRIVING_ELE_MAC_ELE, new CoefficientOffset(DataKey._2306_DRIVING_ELE_MAC_ELE, 0, 10, 10000))
            // 总电流
            .put(DataKey._2614_TOTAL_ELECTRICITY, new CoefficientOffset(DataKey._2614_TOTAL_ELECTRICITY, 0, 10, 10000))
            //
            .put(DataKey._2114, new CoefficientOffset(DataKey._2114, 0, 1, 40))
            //
            .put(DataKey._2115, new CoefficientOffset(DataKey._2115, 0, 1, 40))
            //
            .put(DataKey._2110, new CoefficientOffset(DataKey._2110, 0, 10, 0))
            //
            .put(DataKey._2111, new CoefficientOffset(DataKey._2111, 0, 10, 0))
            //
            .put(DataKey._2119, new CoefficientOffset(DataKey._2119, 0, 10, 0))
            // 单体蓄电池电压值列表
            .put(DataKey._2003_SINGLE_VOLT, new CoefficientOffset(DataKey._2003_SINGLE_VOLT, 1, 1000, 0))
            // 车速
            .put(DataKey._2201_SPEED, new CoefficientOffset(DataKey._2201_SPEED, 0, 10, 0))
            // 累计里程
            .put(DataKey._2202_TOTAL_MILEAGE, new CoefficientOffset(DataKey._2202_TOTAL_MILEAGE, 0, 10, 0))
            // 驱动电机输入电压
            .put(DataKey._2305_DRIVING_ELE_MAC_VOLT, new CoefficientOffset(DataKey._2305_DRIVING_ELE_MAC_VOLT, 0, 10, 0))
            // 车辆电池电压
            .put(DataKey._2403, new CoefficientOffset(DataKey._2403, 0, 5, 0))
            // 进气歧管气压
            .put(DataKey._2405, new CoefficientOffset(DataKey._2405, 0, 100, 0))
            // 燃料喷射压力
            .put(DataKey._2408, new CoefficientOffset(DataKey._2408, 0, 10, 0))
            // 点火提前角
            .put(DataKey._2410, new CoefficientOffset(DataKey._2410, 0, 500, 0))
            // 油门开度
            .put(DataKey._2412, new CoefficientOffset(DataKey._2412, 0, 2, 0))
            //
            .put(DataKey._2413, new CoefficientOffset(DataKey._2413, 0, 100, 0))
            // 经度
            .put(DataKey._2502_LONGITUDE, new CoefficientOffset(DataKey._2502_LONGITUDE, 0, 1000000, 0))
            // 纬度
            .put(DataKey._2503_LATITUDE, new CoefficientOffset(DataKey._2503_LATITUDE, 0, 1000000, 0))
            // 速度
            .put(DataKey._2504, new CoefficientOffset(DataKey._2504, 0, 10, 0))
            // 电池单体电压最高值
            .put(DataKey._2603_SINGLE_VOLT_HIGN_VAL, new CoefficientOffset(DataKey._2603_SINGLE_VOLT_HIGN_VAL, 0, 1000, 0))
            // 电池单体电压最低值
            .put(DataKey._2606_SINGLE_VOLT_LOW_VAL, new CoefficientOffset(DataKey._2606_SINGLE_VOLT_LOW_VAL, 0, 1000, 0))
            // 总电压
            .put(DataKey._2613_TOTAL_VOLTAGE, new CoefficientOffset(DataKey._2613_TOTAL_VOLTAGE, 0, 10, 0))
            // 剩余能量
            .put(DataKey._2616, new CoefficientOffset(DataKey._2616, 0, 10, 0))
            .build();
        // endregion

    /**
     * 偏移系数项 <dataKey, dataKey>
     */
    @NotNull
    private static ImmutableMap<String, CoefficientOffset> COEFFICIENT_OFFSETS = FIXED_COEFFICIENT_OFFSETS;

    static {
        rebuild();
    }

    /**
     * 从数据库构建新的规则, 如果数据库查询异常, 则保留旧的规则.
     */
    private static void initFromDatabase(){

        // 从数据库获取自定义偏移系数项
        final List<CoefficientOffset> offsetsFromDatabase = Conn.getAllCoefOffset();

        if(null == offsetsFromDatabase) {
            return;
        }

        final Map<String, CoefficientOffset> coefficientOffsets =
            Maps.newHashMapWithExpectedSize(offsetsFromDatabase.size() + FIXED_COEFFICIENT_OFFSETS.size());

        for (final CoefficientOffset coefficientOffset : offsetsFromDatabase) {

            if (!FIXED_COEFFICIENT_OFFSETS.containsKey(coefficientOffset.dataKey)) {
                if(!coefficientOffsets.containsKey(coefficientOffset.dataKey)) {
                    coefficientOffsets.put(coefficientOffset.dataKey, coefficientOffset);
                } else {
                    LOG.warn("偏移系数数据项[{}]重复定义.", coefficientOffset.dataKey);
                }
            } else {
                LOG.warn("偏移系数数据项[{}]为固定定义, 忽略外部外部定义.", coefficientOffset.dataKey);
            }
        }

        coefficientOffsets.putAll(FIXED_COEFFICIENT_OFFSETS);
        COEFFICIENT_OFFSETS = ImmutableMap.copyOf(coefficientOffsets);
    }

    public static void rebuild(){

        initFromDatabase();
    }

    @Nullable
    public static CoefficientOffset getCoefficientOffset(@Nullable final String dataKey) {

        if (StringUtils.isBlank(dataKey)) {
            return null;
        }

        return COEFFICIENT_OFFSETS.get(dataKey);
    }
}
