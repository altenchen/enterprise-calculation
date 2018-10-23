package storm.util;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.constant.FormatConstant;
import storm.system.DataKey;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;

/**
 * @author: xzp
 * @date: 2018-06-14
 * @description: 数据表工具箱
 */
@SuppressWarnings("unused")
public final class DataUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DataUtils.class);

    /**
     * 判断报文是否为自动唤醒报文,判断依据：总电压、总电流同时为空则为自动唤醒数据
     * @param data 实时数据
     * @return 是否为自动唤醒报文
     */
    public static boolean isAutoWake(
        @NotNull Map<String,String> data){

        return StringUtils.isBlank(data.get(DataKey._2613_TOTAL_VOLTAGE))
            && StringUtils.isBlank(data.get(DataKey._2614_TOTAL_ELECTRICITY));
    }

    /**
     * 判断报文是否非自动唤醒报文,判断依据：总电压、总电流同时为空则为自动唤醒数据
     * @param data 实时数据
     * @return 是否非自动唤醒报文
     */
    public static boolean isNotAutoWake(
        @NotNull Map<String,String> data){

        return !isAutoWake(data);
    }

    /**
     * 构建经纬度字符串表示
     * @param longitude 经度
     * @param latitude 纬度
     * @return 经纬度
     */
    @Nullable
    @Contract(pure = true)
    public static String buildLocation(
        @Nullable final String longitude,
        @Nullable final String latitude) {

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

    /**
     * 解析平台接收时间
     * @param data
     * @return
     * @throws ParseException
     */
    public static long parsePlatformReceiveTime(
        @NotNull final ImmutableMap<String, String> data)
        throws ParseException {

        final String platformReceiveTimeString = data.get(
            DataKey._9999_PLATFORM_RECEIVE_TIME);

        if (platformReceiveTimeString == null) {
            return 0;
        }

        return parseFormatTime(platformReceiveTimeString);
    }

    /**
     * 解析格式化时间
     * @param formatTimeString 格式化时间
     * @return 时间数值
     * @throws ParseException 解析异常
     */
    public static long parseFormatTime(
        @NotNull final String formatTimeString)
        throws ParseException {

        if(!NumberUtils.isDigits(formatTimeString)) {
            throw new ParseException("formatTimeString must be a digits", 0);
        }

        return DateUtils
            .parseDate(
                formatTimeString,
                new String[]{
                    FormatConstant.DATE_FORMAT
                }
            )
            .getTime();
    }

    /**
     * 解析格式化时间
     * @param formatTimeString 格式化时间
     * @param defaultValue 解析失败返回的默认值
     * @return 时间数值
     */
    public static long parseFormatTime(
        @NotNull final String formatTimeString,
        @NotNull final long defaultValue) {

        try {
            return DateUtils
                .parseDate(
                    formatTimeString,
                    new String[]{
                        FormatConstant.DATE_FORMAT
                    }
                )
                .getTime();
        } catch (final Exception ignore) {
            return defaultValue;
        }
    }

    /**
     * 解析格式化时间
     * @param formatTimeString 格式化时间
     * @param exceptionCallback 解析异常回调
     * @return 时间数值
     */
    public static long parseFormatTime(
        @NotNull final String formatTimeString,
        @NotNull final Function<@NotNull ParseException, @NotNull Long> exceptionCallback) {

        try {
            return DateUtils
                .parseDate(
                    formatTimeString,
                    new String[]{
                        FormatConstant.DATE_FORMAT
                    }
                )
                .getTime();
        } catch (final ParseException e) {
            return exceptionCallback.apply(e);
        }
    }

    /**
     * 构建格式化时间
     * @param milliseconds 时间数值
     * @return 格式化时间
     */
    public static String buildFormatTime(
        final long milliseconds) {

        return DateFormatUtils.format(milliseconds, FormatConstant.DATE_FORMAT);
    }

    /**
     * 构建格式化时间
     * @param date 时间
     * @return 格式化时间
     */
    public static String buildFormatTime(
        final Date date) {

        return DateFormatUtils.format(date, FormatConstant.DATE_FORMAT);
    }

    @Nullable
    public static BigDecimal createBigDecimal(
        @NotNull final String value,
        @Nullable final BigDecimal defaultValue) {

        try
        {
            return NumberUtils.createBigDecimal(value);
        } catch (final NumberFormatException e) {
            return defaultValue;
        }
    }

    @Nullable
    public static BigDecimal createBigDecimal(
        @NotNull final String value,
        @NotNull final Function<@NotNull NumberFormatException, @Nullable BigDecimal> exceptionCallback) {

        try
        {
            return NumberUtils.createBigDecimal(value);
        } catch (final NumberFormatException e) {
            return exceptionCallback.apply(e);
        }
    }

    @Nullable
    @Contract("null -> null")
    public static BigDecimal createBigDecimal(
        @Nullable final String value) {

        if (value == null) {
            return null;
        }
        return createBigDecimal(value, (BigDecimal)null);
    }
}
