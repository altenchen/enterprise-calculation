package storm.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.system.DataKey;
import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-06-14
 * @description: 数据表工具箱
 */
@SuppressWarnings("unused")
public final class DataUtils {

    /**
     * 判断报文是否为自动唤醒报文,判断依据：总电压、总电流同时为空则为自动唤醒数据
     * @param data 集合
     * @return 是否为自动唤醒报文
     */
    public static boolean judgeAutoWake(
        @NotNull Map<String,String> data){

        final String totalVoltage = data.get(DataKey._2613_TOTAL_VOLTAGE);
        final String totalElectricity = data.get(DataKey._2614_TOTAL_ELECTRICITY);
        return StringUtils.isBlank(totalVoltage)
                && StringUtils.isBlank(totalElectricity);
    }

    /**
     * 构建经纬度字符串表示
     * @param longitude 经度
     * @param latitude 纬度
     * @return 经纬度
     */
    @NotNull
    @Contract(pure = true)
    public static String buildLocation(
        @NotNull final String longitude,
        @NotNull final String latitude) {

        if(NumberUtils.isDigits(longitude) && NumberUtils.isDigits(latitude)) {
            return new StringBuilder(32)
                .append(longitude)
                .append(',')
                .append(latitude)
                .toString();
        } else {
            return StringUtils.EMPTY;
        }
    }

    /**
     * 构建经纬度字符串表示
     * @param orientationString 定位状态
     * @param longitudeString 经度
     * @param latitudeString 纬度
     * @return 经纬度
     */
    @NotNull
    @Contract(pure = true)
    public static String buildLocation(
        @Nullable final String orientationString,
        @Nullable final String longitudeString,
        @Nullable final String latitudeString) {
        if(
            NumberUtils.isDigits(orientationString)
                && NumberUtils.isDigits(longitudeString)
                && NumberUtils.isDigits(latitudeString)) {
            return buildLocation(
                NumberUtils.toInt(orientationString),
                NumberUtils.toInt(longitudeString),
                NumberUtils.toInt(latitudeString)
            );
        }

        return StringUtils.EMPTY;
    }

    /**
     * 构建经纬度字符串表示
     * @param orientation 定位状态
     * @param longitude 经度
     * @param latitude 纬度
     * @return 经纬度
     */
    @NotNull
    @Contract(pure = true)
    public static String buildLocation(
        final int orientation,
        final int longitude,
        final int latitude) {

        if(isOrientationUseful(orientation)
            && isOrientationLongitudeUseful(longitude)
            && isOrientationLatitudeUseful(latitude)) {
            return new StringBuilder(32)
                .append(longitude / DataKey.ORIENTATION_PRECISION)
                .append('°')
                .append(isOrientationLongitudeEast(orientation) ? "E" : "W")
                .append(',')
                .append(latitude / DataKey.ORIENTATION_PRECISION)
                .append('°')
                .append(isOrientationLatitudeNorth(orientation) ? "N" : "S")
                .toString();
        }

        return StringUtils.EMPTY;
    }


    /**
     * @param orientationValue 定位状态
     * @return 定位是否有用
     */
    @Contract(pure = true)
    public static boolean isOrientationUseful(int orientationValue) {
        return (orientationValue & DataKey.ORIENTATION_MASK_QUALITY) == DataKey.ORIENTATION_MASK_QUALITY_USEFUL;
    }

    /**
     * @param orientationValue 定位状态
     * @return 定位是否无用
     */
    @Contract(pure = true)
    public static boolean isOrientationUseless(int orientationValue) {
        return (orientationValue & DataKey.ORIENTATION_MASK_QUALITY) == DataKey.ORIENTATION_MASK_QUALITY_USELESS;
    }

    /**
     * @param orientationValue 定位状态
     * @return 是否东经
     */
    @Contract(pure = true)
    public static boolean isOrientationLongitudeEast(int orientationValue) {
        return (orientationValue & DataKey.ORIENTATION_MASK_LONGITUDE) == DataKey.ORIENTATION_MASK_LONGITUDE_EAST;
    }

    /**
     * @param orientationValue 定位状态
     * @return 是否西经
     */
    @Contract(pure = true)
    public static boolean isOrientationLongitudeWest(int orientationValue) {
        return (orientationValue & DataKey.ORIENTATION_MASK_LONGITUDE) == DataKey.ORIENTATION_MASK_LONGITUDE_WEST;
    }

    /**
     * @param latitudeValue 定位状态
     * @return 是否北纬
     */
    @Contract(pure = true)
    public static boolean isOrientationLatitudeNorth(int latitudeValue) {
        return (latitudeValue & DataKey.ORIENTATION_MASK_LATITUDE) == DataKey.ORIENTATION_MASK_LATITUDE_NORTH;
    }

    /**
     * @param latitudeValue 定位状态
     * @return 是否南纬
     */
    @Contract(pure = true)
    public static boolean isOrientationLatitudeSouth(int latitudeValue) {
        return (latitudeValue & DataKey.ORIENTATION_MASK_LATITUDE) == DataKey.ORIENTATION_MASK_LATITUDE_SOUTH;
    }

    /**
     * @param longitude 经度
     * @return 经度是否有用
     */
    @Contract(pure = true)
    public static boolean isOrientationLongitudeUseful(int longitude) {
        return longitude >= DataKey.MIN_2502_LONGITUDE && longitude <= DataKey.MAX_2502_LONGITUDE;
    }

    /**
     * @param latitude 纬度
     * @return 经度是否有用
     */
    @Contract(pure = true)
    public static boolean isOrientationLatitudeUseful(int latitude) {
        return latitude >= DataKey.MIN_2503_LATITUDE && latitude <= DataKey.MAX_2503_LATITUDE;
    }
}
