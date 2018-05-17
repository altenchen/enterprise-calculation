package storm.service;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeFormatService {
	private static ThreadLocal<SimpleDateFormat> formatlocal = new ThreadLocal<SimpleDateFormat>();

	public static long stand = 1510243200000L;//2017-11-10 00:00:00
	public String toDateString(Date date){
		if(null == date)
			return null;
		SimpleDateFormat format = getDateFormat();
		return format.format(date);
	}
	
	public long stringTimeLong(String yyyyMMddHHmmss){
		if(null == yyyyMMddHHmmss)
			return 0;
		try {
			SimpleDateFormat format = getDateFormat();
			Date date = format.parse(yyyyMMddHHmmss);
			return date.getTime();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	private SimpleDateFormat getDateFormat(){
		SimpleDateFormat format = formatlocal.get();
		try {
			if (null == format) {
				format = new SimpleDateFormat("yyyyMMddHHmmss");
				formatlocal.set(format);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return formatlocal.get();
	}
	
	public static boolean nowNearNextZeroTime(){
		return nowNearNextZeroTime(System.currentTimeMillis());
	}
	
	/**
	 * 新的一天开始2分钟分钟数据不会计入，只会清理一次
	 * @param now
	 * @return
	 */
	public static boolean newDayNearZeroTime(long now){
		long space = now - stand;
		int days = (int)(space/86400000);
		if (now-(stand+86400000L*(days)) < 150000) {// 150秒钟;300000 5分钟
			return true;
		}
		return false;
	}
	/**
	 * 每天最后的2分钟数据不会计入
	 * @param now
	 * @return
	 */
	public static boolean nowNearNextZeroTime(long now){
		long space = now - stand;
		int days = (int)(space/86400000);
		if ((stand+86400000L*(days+1)-now) < 150000) {// 150秒钟;300000 5分钟
			return true;
		}
		return false;
	}
	/**
	 * 
	 * @param now 当前时间
	 * @param nearMillis 距离0点相差的时间段
	 * @return
	 */
	public static boolean nowNearNextZeroTime(long now,long nearMillis){
		long space = now - stand;
		int days = (int)(space/86400000);
		if ((stand+86400000L*(days+1)-now) < nearMillis) {//nearMillis相差时间
			return true;
		}
		return false;
	}
	
	public static void main(String[] args) {
		TimeFormatService service = new TimeFormatService();
		long time=service.stringTimeLong("20171111000319");
		System.out.println(time);
		System.out.println(new Date(time));
		System.out.println(new Date(time+86400000));
		
		String serTime = service.toDateString(new Date());
		serTime = serTime.substring(0,serTime.length()-6);
		System.out.println(serTime);
		
		System.out.println(TimeFormatService.newDayNearZeroTime(new Date(time).getTime()));
	}
}
