package storm.service;

import org.jetbrains.annotations.NotNull;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 时间格式化服务
 * 精确到毫秒
 */
public final class TimeFormatService {
    private static ThreadLocal<SimpleDateFormat> formatlocal = new ThreadLocal<SimpleDateFormat>();

    private static final long oneDayTotalMillisecond = 1000 * 60 * 60 * 24;

    //2017-11-10 00:00:00
    private static long stand = 1510243200000L;

    @NotNull
    public String toDateString(@NotNull Date date){
        SimpleDateFormat format = getDateFormat(formatlocal);
        return format.format(date);
    }
    
    public long stringTimeLong(@NotNull String yyyyMMddHHmmss)
        throws ParseException {

        SimpleDateFormat format = getDateFormat(formatlocal);
        Date date = format.parse(yyyyMMddHHmmss);
        return date.getTime();
    }

    @NotNull
    private static SimpleDateFormat getDateFormat(@NotNull ThreadLocal<SimpleDateFormat> formatlocal) {
        SimpleDateFormat format = formatlocal.get();
        if (null == format) {
            format = new SimpleDateFormat("yyyyMMddHHmmss");
            formatlocal.set(format);
        }
        return format;
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
//        long space = now - stand;
//        int days = (int)(space/86400000);
//        if (now-(stand+86400000L*(days)) < 150000) {// 150秒钟;300000 5分钟
//            return true;
//        }
//        return false;

        return (now % oneDayTotalMillisecond) < (1000 * 150);
    }
    /**
     * 每天最后的2分钟数据不会计入
     * @param now
     * @return
     */
    public static boolean nowNearNextZeroTime(long now){
//        long space = now - stand;
//        int days = (int)(space/86400000);
//        if ((stand+86400000L*(days+1)-now) < 150000) {// 150秒钟;300000 5分钟
//            return true;
//        }
//        return false;


        return (now % oneDayTotalMillisecond) > (oneDayTotalMillisecond - 1000 * 150);
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
        try {
            long time=service.stringTimeLong("20171111000319");
            System.out.println(time);
            System.out.println(new Date(time));
            System.out.println(new Date(time+86400000));

            String serTime = service.toDateString(new Date());
            serTime = serTime.substring(0,serTime.length()-6);
            System.out.println(serTime);

            System.out.println(TimeFormatService.newDayNearZeroTime(new Date(time).getTime()));
        }
        catch (ParseException ex) {
            ex.printStackTrace();
        }
    }
}
