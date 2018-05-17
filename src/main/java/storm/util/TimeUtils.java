package storm.util;

import java.util.Calendar;

public class TimeUtils {
	static Calendar cal;
	static{
		cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 24);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
	}
	public static String fillNBitBefore(String s, int length, String fillchar) {
		if (ObjectUtils.isNullOrEmpty(s)) {
			s ="0";
		}
		StringBuilder builder=new StringBuilder();
		int len = s.length();
        for (int i = 0; i < length-len; i++) {
            builder.append(fillchar);
        }
        builder.append(s);
        return builder.toString();
    }

    
    public static String fillNBitAfter(String s, int length, String fillchar) {
    	StringBuilder builder=new StringBuilder(s);
        for (int i = 0; i < length; i++) {
            if (builder.length() >= length) {
                break;
            }
            builder.append(fillchar);
        }
        return builder.toString();
    }

   
    public static long getNextDayUTC() {
        return cal.getTimeInMillis();
    }
}
