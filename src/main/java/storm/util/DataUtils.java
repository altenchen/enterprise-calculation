package storm.util;

import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

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
        if(StringUtils.isNotBlank(longitude) && StringUtils.isNotBlank(latitude)) {
            return new StringBuilder(longitude).append(',').append(latitude).toString();
        } else {
            return StringUtils.EMPTY;
        }
    }
}
