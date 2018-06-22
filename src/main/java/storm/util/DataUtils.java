package storm.util;

import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import storm.system.DataKey;

import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-06-14
 * @description: 数据表工具箱
 */
public final class DataUtils {

    /**
     * 构建经纬度字符串表示
     * @param longitude 经度
     * @param latitude 纬度
     * @return 经纬度
     */
    @NotNull
    @Contract(pure = true)
    public static final String buildLocation(@NotNull final String longitude, @NotNull final String latitude) {
        return longitude + "," + latitude;
    }

    /**
     * 判断报文是否为自动唤醒报文,判断依据：总电压、总电流同时为空则为自动唤醒数据
     * @param map 集合
     * @return 是否为自动唤醒报文
     */
    public static boolean judgeAutoWake(@NotNull Map map){
        String totalVoltage = (String)map.get(DataKey._2613_TOTAL_VOLTAGE);
        String totalElectricity = (String)map.get(DataKey._2614_TOTAL_ELECTRICITY);
        return StringUtils.isBlank(totalVoltage)
                && StringUtils.isBlank(totalElectricity);
    }
}
