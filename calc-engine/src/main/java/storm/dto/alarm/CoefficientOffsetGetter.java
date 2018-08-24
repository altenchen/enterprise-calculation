package storm.dto.alarm;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;
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
public class CoefficientOffsetGetter {

    private static final Logger LOG = LoggerFactory.getLogger(CoefficientOffsetGetter.class);

    /**
     * 全局偏移系数项 dataKey
     */
    private static final Set<String> GLOBAL_ITEMS = new HashSet<>();

    /**
     * 自定义偏移系数项 dataKey
     */
    private static final Set<String> CUSTOM_ITEMS = new HashSet<>();

    /**
     * 偏移系数项 <dataKey, dataKey>
     */
    private static final Map<String, CoefficientOffset> COEFFICIENT_OFFSET_MAP = new ConcurrentHashMap<>();

    private static final Conn conn = new Conn();

    static {
        initGlobal();
        rebuild();
    }

    /**
     * 初始化全局偏移系数
     */
    private static void initGlobal(){

        // 电机控制器温度
        initGlobal(DataKey._2302_DRIVING_ELE_MAC_TEMPCTOL, 0, 1, 40);
        // 驱动电机温度
        initGlobal(DataKey._2304_DRIVING_ELE_MAC_TEMP, 0, 1, 40);
        // ECU温度
        initGlobal(DataKey._2402, 0, 1, 40);
        // 发动机温度
        initGlobal(DataKey._2404, 0, 1, 40);
        // 进气温度
        initGlobal(DataKey._2406, 0, 1, 40);
        // 废气排出温度
        initGlobal(DataKey._2407, 0, 1, 40);
        // 电池单体最高温度值
        initGlobal(DataKey._2609_SINGLE_HIGNTEMP_VAL, 0, 1, 40);
        // 电池单体最低温度值
        initGlobal(DataKey._2612_SINGLE_LOWTEMP_VAL, 0, 1, 40);
        // 单体温度值列表
        initGlobal(DataKey._2103_SINGLE_TEMP, 0, 1, 40);
        // 驱动电机母线电流
        initGlobal(DataKey._2306_DRIVING_ELE_MAC_ELE, 0, 10, 10000);
        // 总电流
        initGlobal(DataKey._2614_TOTAL_ELECTRICITY, 0, 10, 10000);
        //
        initGlobal(DataKey._2114, 0, 1, 40);
        //
        initGlobal(DataKey._2115, 0, 1, 40);
        //
        initGlobal(DataKey._2110, 0, 10, 0);
        //
        initGlobal(DataKey._2111, 0, 10, 0);
        //
        initGlobal(DataKey._2119, 0, 10, 0);
        // 单体蓄电池电压值列表
        initGlobal(DataKey._2003_SINGLE_VOLT, 1, 1000, 0);
        // 车速
        initGlobal(DataKey._2201_SPEED, 0, 10, 0);
        // 累计里程
        initGlobal(DataKey._2202_TOTAL_MILEAGE, 0, 10, 0);
        // 驱动电机输入电压
        initGlobal(DataKey._2305_DRIVING_ELE_MAC_VOLT, 0, 10, 0);
        // 车辆电池电压
        initGlobal(DataKey._2403, 0, 5, 0);
        // 进气歧管气压
        initGlobal(DataKey._2405, 0, 100, 0);
        // 燃料喷射压力
        initGlobal(DataKey._2408, 0, 10, 0);
        // 点火提前角
        initGlobal(DataKey._2410, 0, 500, 0);
        // 油门开度
        initGlobal(DataKey._2412, 0, 2, 0);
        //
        initGlobal(DataKey._2413, 0, 100, 0);
        // 经度
        initGlobal(DataKey._2502_LONGITUDE, 0, 1000000, 0);
        // 纬度
        initGlobal(DataKey._2503_LATITUDE, 0, 1000000, 0);
        // 速度
        initGlobal(DataKey._2504, 0, 10, 0);
        // 电池单体电压最高值
        initGlobal(DataKey._2603_SINGLE_VOLT_HIGN_VAL, 0, 1000, 0);
        // 电池单体电压最低值
        initGlobal(DataKey._2606_SINGLE_VOLT_LOW_VAL, 0, 1000, 0);
        // 总电压
        initGlobal(DataKey._2613_TOTAL_VOLTAGE, 0, 10, 0);
        // 剩余能量
        initGlobal(DataKey._2616, 0, 10, 0);
    }

    private static void initGlobal(
        @NotNull final String dataKey,
        final int type,
        final double coef,
        final double offset) {

        final CoefficientOffset item = new CoefficientOffset(dataKey, type, coef, offset);
        COEFFICIENT_OFFSET_MAP.put(dataKey, item);
        GLOBAL_ITEMS.add(dataKey);
    }

    /**
     * 将所有自定义规则替换为数据库中的新规则
     * 问题: 非自定义规则一但录入, 则不允许
     */
    private static void initCustom(){

        // 从数据库获取自定义偏移系数项
        final List<CoefficientOffset> offsetsFromDatabase = conn.getAllCoefOffset();

        final Set<String> addItems = new HashSet<>();
        if (CollectionUtils.isNotEmpty(offsetsFromDatabase)) {

            for (final CoefficientOffset coefficientOffset : offsetsFromDatabase) {

                if (!GLOBAL_ITEMS.contains(coefficientOffset.dataKey)) {

                    COEFFICIENT_OFFSET_MAP.put(coefficientOffset.dataKey, coefficientOffset);

                    CUSTOM_ITEMS.add(coefficientOffset.dataKey);

                    addItems.add(coefficientOffset.dataKey);
                } else {
                    LOG.warn("数据项[{}]为固定定义, 忽略外部偏移系数.", coefficientOffset.dataKey);
                }
            }
        }

        // 清理不是本次查出来的非固定规则

        final HashSet<String> needRemove = new HashSet<>();
        for (final String dataKey : CUSTOM_ITEMS) {
            if (!addItems.contains(dataKey)) {
                needRemove.add(dataKey);
            }
        }
        for (final String dataKey : needRemove) {
            COEFFICIENT_OFFSET_MAP.remove(dataKey);
            CUSTOM_ITEMS.remove(dataKey);
        }
    }

    public static void rebuild(){

        initCustom();
    }

    @Nullable
    public static CoefficientOffset getCoefficientOffset(@Nullable final String dataKey) {

        if (StringUtils.isBlank(dataKey)) {
            return null;
        }
        return COEFFICIENT_OFFSET_MAP.get(dataKey);
    }
}
