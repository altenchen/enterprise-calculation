package storm.dto;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class TimePeriod implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 11263L;
	private static ThreadLocal<Calendar> callocal = new ThreadLocal<Calendar>();
	private static ThreadLocal<SimpleDateFormat> formatlocal = new ThreadLocal<SimpleDateFormat>();
	public static final String TYPE_MONTH="M";//月
	public static final String TYPE_WEEK="W";//周
	public static final String TYPE_DAY="D";//天
	public static final String TYPE_SP="SP";//区间段
	public String type;
	public List<Long> inbigtimes;
	public List<Long[]> inSeqbigtimes;
	public List<Long[]> inSeqSmalltimes;
	public TimePeriod(String type, List<Long> inbigtimes, List<Long[]> inSeqbigtimes, List<Long[]> inSeqSmalltimes) {
		super();
		this.type = type;
		this.inbigtimes = inbigtimes;
		this.inSeqbigtimes = inSeqbigtimes;
		this.inSeqSmalltimes = inSeqSmalltimes;
	}
	public boolean nowIntimes(){
		return dateIntimes(new Date());
	}
	
	public boolean dateIntimes(Date date){
		if(null == type)
			return false;
		if (null == inSeqSmalltimes) 
			return false;
		try {
			Calendar cal = getCalendar();
			cal.setTime(date);
			if(TimePeriod.TYPE_SP.equalsIgnoreCase(type)){
				int year = cal.get(Calendar.YEAR);
				int month = cal.get(Calendar.MONTH)+1;
				int day = cal.get(Calendar.DAY_OF_MONTH);
				boolean inBig = false;
				if(null != inSeqbigtimes)
				for(Long[] bigSeq:inSeqbigtimes){
					if (bigSeq[0] <= year*10000+month*100+day 
							&& year*10000+month*100+day <= bigSeq[1]) {
						inBig = true;
						break;
					}
				}
				if (!inBig) 
					return false;
				int hour = cal.get(Calendar.HOUR_OF_DAY);
				int minute = cal.get(Calendar.MINUTE);
				for(Long[]small : inSeqSmalltimes){
					if (small[0]<=hour*100+minute && hour*100+minute<=small[1]) 
						return true;
				}
				return false;
				
			} else if(TimePeriod.TYPE_DAY.equalsIgnoreCase(type)){

				int hour = cal.get(Calendar.HOUR_OF_DAY);
				int minute = cal.get(Calendar.MINUTE);
				for(Long[]small : inSeqSmalltimes){
					if (small[0]<=hour*100+minute && hour*100+minute<=small[1]) 
						return true;
				}
				return false;
			} else if(TimePeriod.TYPE_WEEK.equalsIgnoreCase(type)){
				int weekday = cal.get(Calendar.DAY_OF_WEEK);
				weekday = weekday-1;
				if (0 == weekday) 
					weekday = 7;
				boolean inBig = false;
				if(null != inSeqbigtimes)
				for(Long[] bigSeq:inSeqbigtimes){
					if (bigSeq[0] <= weekday && weekday <= bigSeq[1]) {
						inBig = true;
						break;
					}
				}
				if (!inBig) {
					if(null != inbigtimes)
					for (long bigi : inbigtimes) {
						if(bigi == weekday){
							inBig = true;
							break;
						}
					}
				}
				if (!inBig) 
					return false;
				int hour = cal.get(Calendar.HOUR_OF_DAY);
				int minute = cal.get(Calendar.MINUTE);
				for(Long[]small : inSeqSmalltimes){
					if (small[0]<=hour*100+minute && hour*100+minute<=small[1]) 
						return true;
				}
				return false;
				
			} else if(TimePeriod.TYPE_MONTH.equalsIgnoreCase(type)){
				int day = cal.get(Calendar.DAY_OF_MONTH);
				boolean inBig = false;
				if(null != inSeqbigtimes)
				for(Long[] bigSeq:inSeqbigtimes){
					if (bigSeq[0] <= day && day <= bigSeq[1]) {
						inBig = true;
						break;
					}
				}
				if (!inBig) {
					if(null != inbigtimes)
					for (long bigi : inbigtimes) {
						if(bigi == day){
							inBig = true;
							break;
						}
					}
				}
				if (!inBig) 
					return false;
				int hour = cal.get(Calendar.HOUR_OF_DAY);
				int minute = cal.get(Calendar.MINUTE);
				for(Long[]small : inSeqSmalltimes){
					if (small[0]<=hour*100+minute && hour*100+minute<=small[1]) 
						return true;
				}
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean stringTimeIntimes(String yyyyMMddHHmmss){
		if(null == type)
			return false;
		if (null == inSeqSmalltimes) 
			return false;
		if (null == yyyyMMddHHmmss 
				|| "".equals(yyyyMMddHHmmss.trim())
				|| yyyyMMddHHmmss.length() < 12) 
			return false;
		
		try {
			int hm = Integer.valueOf(yyyyMMddHHmmss.substring(8, 12));
			if(TimePeriod.TYPE_SP.equalsIgnoreCase(type)){
				int yMd = Integer.valueOf(yyyyMMddHHmmss.substring(0, 8));
				boolean inBig = false;
				for(Long[] bigSeq:inSeqbigtimes){
					if (bigSeq[0] <= yMd
							&& yMd <= bigSeq[1]) {
						inBig = true;
						break;
					}
				}
				if (!inBig) 
					return false;
				for(Long[]small : inSeqSmalltimes){
					if (small[0]<=hm && hm<=small[1]) 
						return true;
				}
				return false;
			} else if(TimePeriod.TYPE_DAY.equalsIgnoreCase(type)){
				for(Long[]small : inSeqSmalltimes){
					if (small[0]<=hm && hm<=small[1]) 
						return true;
				}
				return false;
			} else {
				SimpleDateFormat format = getDateFormat();
				Date date = format.parse(yyyyMMddHHmmss);
				Calendar cal = getCalendar();
				cal.setTime(date);
				if(TimePeriod.TYPE_WEEK.equalsIgnoreCase(type)){
					int weekday = cal.get(Calendar.DAY_OF_WEEK);
					weekday = weekday-1;
					if (0 == weekday) 
						weekday = 7;
					boolean inBig = false;
					if(null != inSeqbigtimes)
					for(Long[] bigSeq:inSeqbigtimes){
						if (bigSeq[0] <= weekday && weekday <= bigSeq[1]) {
							inBig = true;
							break;
						}
					}
					if (!inBig) {
						if(null != inbigtimes)
						for (long bigi : inbigtimes) {
							if(bigi == weekday){
								inBig = true;
								break;
							}
						}
					}
					if (!inBig) 
						return false;
					for(Long[]small : inSeqSmalltimes){
						if (small[0]<=hm && hm<=small[1]) 
							return true;
					}
					return false;
				} else if(TimePeriod.TYPE_MONTH.equalsIgnoreCase(type)){
					int day = cal.get(Calendar.DAY_OF_MONTH);
					boolean inBig = false;
					if(null != inSeqbigtimes)
					for(Long[] bigSeq:inSeqbigtimes){
						if (bigSeq[0] <= day && day <= bigSeq[1]) {
							inBig = true;
							break;
						}
					}
					if (!inBig) {
						if(null != inbigtimes)
						for (long bigi : inbigtimes) {
							if(bigi == day){
								inBig = true;
								break;
							}
						}
					}
					if (!inBig) 
						return false;
					for(Long[]small : inSeqSmalltimes){
						if (small[0]<=hm && hm<=small[1]) 
							return true;
					}
					return false;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
			
		return false;
	}
	
	public boolean longtimeIntimes(long utc){
		if(null == type)
			return false;
		if (null == inSeqSmalltimes) 
			return false;
		return dateIntimes(new Date(utc));
	}
	boolean isNumber(String str) {
        if (!StringUtils.isEmpty(str) && str.matches("[0-9]*.[0-9]{0,10}")) {
            return true;
        }
        return false;
    }
	
	private Calendar getCalendar(){
		Calendar cal = callocal.get();
		if (null == cal) {
			cal = Calendar.getInstance();
			callocal.set(cal);
		}
		return callocal.get();
	}
	
	private SimpleDateFormat getDateFormat(){
		SimpleDateFormat format = formatlocal.get();
		if (null == format) {
			format = new SimpleDateFormat("yyyyMMddHHmmss");
			formatlocal.set(format);
		}
		return formatlocal.get();
	}
	
}
