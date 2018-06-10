package storm.util;

/**
 *	数字格式校验工具
 * @author xzp
 */
public class NumberUtils {

	private static final String NUMBER_REGEX = "[0-9]*.[0-9]*";

	/**
	 * @param str 数字格式的字符串
	 * @return 如果是合法数字格式, 则返回字符串本身, 否则返回"0"
	 */
	public static String stringNumber(String str) {
        if (!ObjectUtils.isNullOrEmpty(str) && str.matches(NUMBER_REGEX)) {
            return str;
        }
        return "0";
    }

	/**
	 * @param str 数字格式的字符串
	 * @return 字符串格式是否合法
	 */
	public static boolean stringIsNumber(String str) {
		if (!ObjectUtils.isNullOrEmpty(str) && str.trim().matches(NUMBER_REGEX)) {
			return true;
		}
		return false;
	}
}
