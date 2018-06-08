package storm.util;

public class NumberUtils {
	public static String stringNumber(String str) {
        if (!ObjectUtils.isNullOrEmpty(str) && str.matches("[0-9]*.[0-9]*")) {
            return str;
        }
        return "0";
    }
	
	public static boolean stringIsNumber(String str) {
		if (!ObjectUtils.isNullOrEmpty(str) && str.trim().matches("[0-9]*.[0-9]*")) {
			return true;
		}
		return false;
	}

	public static boolean tryParseLong(String str, Reference<Long> value) {

		if(null == str) {
		    return false;
        }

        if(!str.matches("[0-9]+(?:\\.[0-9]+)?")) {
		    return false;
        }

        value.value = Long.parseLong(str);
		return true;
	}
}
