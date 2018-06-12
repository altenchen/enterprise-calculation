package storm.dto.fence;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import storm.dto.TimePeriod;
import storm.handler.fence.input.Rule;
import storm.util.ObjectUtils;

/**
 * @author wza
 * 电子围栏处理
 */
public class EleFence implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 119000001L;
//	private static ThreadLocal<Calendar> callocal = new ThreadLocal<Calendar>();
	private static ThreadLocal<SimpleDateFormat> formatlocal = new ThreadLocal<SimpleDateFormat>();
	
	public String id;//围栏编号
	public String name;//围栏名称
	public String type;//围栏类型 1:矩形,2:圆,3:多边形,4:线段
	public String status;//围栏是否有效 1生效
	public String operType;//操作类型
	public String[] timeQuantum;//{starttime1|endtime1,starttime1|endtime1,...}
	public List<Segment> segments;//线段列簇
	public final List<PolygonCal> polygons = new LinkedList<>();//多边形簇
	public List<Circle> circles;//圆形簇
	public List<Rule>rules;//规则
	public Map<String, Object> judge;//判定结果
	public String pointRange;
	public String timesegs;
	public String periodValidtime;
	public TimePeriod period;
	public boolean isalive;
	public long lastalivetime;
	{
		status = "0";
		isalive = false;
		lastalivetime = 0L;
	}
	public void build(){
		try {
			if(null != timesegs){
				int copit=timesegs.indexOf(",");
				String [] strings = null;
				if(copit > 0){
					strings=timesegs.split(",");
				} else {
					strings = new String[]{timesegs};
				}
				timeQuantum = new String[strings.length];
				for (int i = 0; i < strings.length; i++) {
					if (null == strings[i] || "".equals(strings[i].trim())) {
                        continue;
                    }
					String string = strings[i];
					string = string.replace("-", "")
							.replace(":", "")
							.replace(" ", "");
					timeQuantum[i] = string;
				}
			}
			
			if(null != pointRange){
				String [] pits = pointRange.trim().split(";");
				boolean isnum = isNumber(pits[0]);
				if (isnum) {
					if("2".equals(type.trim())){
						double radius = Double.valueOf(pits[0]);
						String [] lanlons = pits[1].split(",");
						double x = 1000000*Double.valueOf(lanlons[0]);
						double y = 1000000*Double.valueOf(lanlons[1]);
						Coordinate center = new Coordinate(x, y);
						Circle circle = new Circle(center, radius);
						if(null == circles) {
                            circles = new LinkedList<Circle>();
                        }
						circles.add(circle);
					}
					if("4".equals(type.trim())){
						double width = Double.valueOf(pits[0]);
						if(null == segments) {
                            segments = new LinkedList<Segment>();
                        }
						for (int i = 1; i < pits.length-1; i++) {
							String [] lanlons = pits[i].split(",");
							double x = 1000000*Double.valueOf(lanlons[0]);
							double y = 1000000*Double.valueOf(lanlons[1]);
							Coordinate start = new Coordinate(x, y);
							
							String [] lanlone = pits[i+1].split(",");
							double xe = 1000000*Double.valueOf(lanlone[0]);
							double ye = 1000000*Double.valueOf(lanlone[1]);
							Coordinate end = new Coordinate(xe, ye);
							
							Segment segment = new Segment(start, end, width);
							segments.add(segment);
						}
					}
				}
				
				if ("1".equals(type.trim()) || "3".equals(type.trim())) {
					
					PolygonCal polygon = new PolygonCal();
					
					for (int i = 0; i < pits.length; i++) {
						String [] lanlons = pits[i].split(",");
						double x = 1000000*Double.valueOf(lanlons[0]);
						double y = 1000000*Double.valueOf(lanlons[1]);
						Coordinate coord = new Coordinate(x, y);
						
						polygon.addCoordinate(coord);
					}
					
					polygons.add(polygon);
				}
			}
			if(null != periodValidtime && !"".equals(periodValidtime.trim())){
				String [] periodTimes = periodValidtime.split(":");
				if (3 == periodTimes.length
						&& null != periodTimes[0] && !"".equals(periodTimes[0].trim())
								&& null != periodTimes[1] && !"".equals(periodTimes[1].trim())
										&& null != periodTimes[2] && !"".equals(periodTimes[2].trim())) {
					String type = periodTimes[0];
					String timebigseq = periodTimes[1];
					List<Long>bsingle=new LinkedList<Long>();
					List<Long[]>bseqs=new LinkedList<Long[]>();
					String []bigseqs = timebigseq.split(",");
					for (String str : bigseqs) {
						if (str.indexOf("-")>0) {
							String [] sted = str.split("-");
							if(2 == sted.length) {
                                bseqs.add(new Long[]{Long.valueOf(sted[0]),Long.valueOf(sted[1])});
                            }
							
						} else {
							bsingle.add(Long.valueOf(str));
						}
					}
					String timesmallseq = periodTimes[2];
					List <Long[]> smallSeq = new LinkedList<Long[]>();
					String[]smalls=null;
					if (timesmallseq.indexOf(",")>0) {
                        smalls = timesmallseq.split(",");
                    } else {
                        smalls = new String[]{timesmallseq};
                    }
					
					for (String str : smalls) {
						String [] sted = str.split("-");
						if(2 == sted.length) {
                            smallSeq.add(new Long[]{Long.valueOf(sted[0]),Long.valueOf(sted[1])});
                        }
						
					}
					period = new TimePeriod(type, bsingle, bseqs, smallSeq);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public boolean coordisIn(Coordinate coord){
		if(null != circles && circles.size()>0){
			for (Circle circle : circles) {
				if(isPointInCircle(circle, coord)) {
                    return true;
                }
				
			}
		}
		if(null != segments && segments.size()>0){
			for (Segment segment : segments) {
				if (isPointInSegment(segment, coord)) {
                    return true;
                }
				
			}
		}
		
		if(null != polygons && polygons.size()>0){
			for (PolygonCal polygon : polygons) {
				if (isPointInPolygon(polygon.coords,coord)) {
                    return true;
                }
				
			}
		}
		return false;
	}
	
	private boolean isPointInCircle(Circle circle,Coordinate coord){
    	if (null == circle || 0 == circle.radius || null == coord || -999 == coord.x || -999 == coord.y) {
            return false;
        }
    	return ( Math.pow(coord.x-circle.center.x,2)+Math.pow(coord.y-circle.center.y,2) )<=Math.pow(circle.radius,2);
    }
	
	private boolean isPointInSegment(Segment segment,Coordinate coord){
    	if (null == segment || null == segment.st || null == segment.ed 
    			|| -999 == segment.st.x || -999 == segment.st.y 
    			|| -999 == segment.ed.x || -999 == segment.ed.y
    			|| null == coord || -999 == coord.x || -999 == coord.y) {
            return false;
        }
    	if(segment.st.x==segment.ed.x && segment.st.y==segment.ed.y) {
            return false;
        }
    	
    	Circle circle=new Circle(segment.st, segment.width);
    	boolean in = isPointInCircle(circle,coord);
    	if (in) {
            return in;
        }
    	
    	circle=new Circle(segment.ed, segment.width);
    	in = isPointInCircle(circle,coord);
    	if (in) {
            return in;
        }
    	double xmin = Math.min(segment.st.x, segment.ed.x);
    	double ymin = Math.min(segment.st.y, segment.ed.y);
    	
    	double xmax = Math.max(segment.st.x, segment.ed.x);
    	double ymax = Math.max(segment.st.y, segment.ed.y);
    	
    	if(coord.x < xmin-segment.width || coord.x > xmax+segment.width
    			|| coord.y < ymin-segment.width || coord.y > ymax+segment.width) {
            return false;
        }
    	
    	if(segment.st.x==segment.ed.x) {
            return Math.abs(coord.x-segment.st.x)<= segment.width;
        }
    	double k=(segment.ed.y-segment.st.y)/(segment.ed.x-segment.st.x);
    	double c=(segment.ed.x*segment.st.y-segment.st.x*segment.ed.y)/(segment.ed.x-segment.st.x);
    	double distance=Math.abs(k*coord.x-coord.y+c)/Math.sqrt(k*k+1);
    	
    	return distance<= segment.width;
    }
	
	/**
     * 判断 点是否在多边形的内部
     * @param coords
     * @param coord
     * @return
     */
    private boolean isPointInPolygon(List<Coordinate> coords, Coordinate coord) {  
        int i, j;  
        boolean c = false;  
        for (i = 0, j = coords.size() - 1; i < coords.size(); j = i++) {  
            if ((((coords.get(i).x <= coord.x) && (coord.x < coords  
                    .get(j).x)) || ((coords.get(j).x <= coord.x) && (coord.x < coords  
                    .get(i).x)))  
                    && (coord.y < (coords.get(j).y - coords.get(i).y)  
                            * (coord.x - coords.get(i).x)  
                            / (coords.get(j).x - coords.get(i).x)  
                            + coords.get(i).y)) {  
                c = !c;  
            }  
        }  
        return c;  
    }
    
	public boolean nowIsalive(){
		long now = System.currentTimeMillis();
		if (now - lastalivetime > 300000) {//围栏每5分钟判断是否处于激活状态
			isalive = nowIntimes();
			lastalivetime = now;
		} 
		return isalive;
	}
	
	public boolean nowIntimes(){
		boolean nowin = nowIntimeQuantum();
		if (nowin) {
            return nowin;
        }
		
		return dateIntimes(new Date());
	}
	
	public boolean dateIntimes(Date date){
		if(null == period || null == period.type) {
            return false;
        }
		return period.dateIntimes(date);
//		if (null == period.inSeqSmalltimes) 
//			return false;
//		try {
//			Calendar cal = getCalendar();
//			cal.setTime(date);
//			if(TimePeriod.TYPE_SP.equalsIgnoreCase(period.type)){
//				int year = cal.get(Calendar.YEAR);
//				int month = cal.get(Calendar.MONTH)+1;
//				int day = cal.get(Calendar.DAY_OF_MONTH);
//				boolean inBig = false;
//				if(null != period.inSeqbigtimes)
//				for(Long[] bigSeq:period.inSeqbigtimes){
//					if (bigSeq[0] <= year*10000+month*100+day 
//							&& year*10000+month*100+day <= bigSeq[1]) {
//						inBig = true;
//						break;
//					}
//				}
//				if (!inBig) 
//					return false;
//				int hour = cal.get(Calendar.HOUR_OF_DAY);
//				int minute = cal.get(Calendar.MINUTE);
//				for(Long[]small : period.inSeqSmalltimes){
//					if (small[0]<=hour*100+minute && hour*100+minute<=small[1]) 
//						return true;
//				}
//				return false;
//				
//			} else if(TimePeriod.TYPE_DAY.equalsIgnoreCase(period.type)){
//
//				int hour = cal.get(Calendar.HOUR_OF_DAY);
//				int minute = cal.get(Calendar.MINUTE);
//				for(Long[]small : period.inSeqSmalltimes){
//					if (small[0]<=hour*100+minute && hour*100+minute<=small[1]) 
//						return true;
//				}
//				return false;
//			} else if(TimePeriod.TYPE_WEEK.equalsIgnoreCase(period.type)){
//				int weekday = cal.get(Calendar.DAY_OF_WEEK);
//				weekday = weekday-1;
//				if (0 == weekday) 
//					weekday = 7;
//				boolean inBig = false;
//				if(null != period.inSeqbigtimes)
//				for(Long[] bigSeq:period.inSeqbigtimes){
//					if (bigSeq[0] <= weekday && weekday <= bigSeq[1]) {
//						inBig = true;
//						break;
//					}
//				}
//				if (!inBig) {
//					if(null != period.inbigtimes)
//					for (long bigi : period.inbigtimes) {
//						if(bigi == weekday){
//							inBig = true;
//							break;
//						}
//					}
//				}
//				if (!inBig) 
//					return false;
//				int hour = cal.get(Calendar.HOUR_OF_DAY);
//				int minute = cal.get(Calendar.MINUTE);
//				for(Long[]small : period.inSeqSmalltimes){
//					if (small[0]<=hour*100+minute && hour*100+minute<=small[1]) 
//						return true;
//				}
//				return false;
//				
//			} else if(TimePeriod.TYPE_MONTH.equalsIgnoreCase(period.type)){
//				int day = cal.get(Calendar.DAY_OF_MONTH);
//				boolean inBig = false;
//				if(null != period.inSeqbigtimes)
//				for(Long[] bigSeq:period.inSeqbigtimes){
//					if (bigSeq[0] <= day && day <= bigSeq[1]) {
//						inBig = true;
//						break;
//					}
//				}
//				if (!inBig) {
//					if(null != period.inbigtimes)
//					for (long bigi : period.inbigtimes) {
//						if(bigi == day){
//							inBig = true;
//							break;
//						}
//					}
//				}
//				if (!inBig) 
//					return false;
//				int hour = cal.get(Calendar.HOUR_OF_DAY);
//				int minute = cal.get(Calendar.MINUTE);
//				for(Long[]small : period.inSeqSmalltimes){
//					if (small[0]<=hour*100+minute && hour*100+minute<=small[1]) 
//						return true;
//				}
//				return false;
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return false;
	}
	
	public boolean stringTimeIntimes(String yyyyMMddHHmmss){
		boolean intime = intimeQuantum(yyyyMMddHHmmss);
		if (intime) {
            return intime;
        }
		
		if(null == period || null == period.type) {
            return false;
        }
		return period.stringTimeIntimes(yyyyMMddHHmmss);
//		if (null == period.inSeqSmalltimes) 
//			return false;
//		if (null == yyyyMMddHHmmss 
//				|| "".equals(yyyyMMddHHmmss.trim())
//				|| yyyyMMddHHmmss.length() < 12) 
//			return false;
//		
//		try {
//			int hm = Integer.valueOf(yyyyMMddHHmmss.substring(8, 12));
//			if(TimePeriod.TYPE_SP.equalsIgnoreCase(period.type)){
//				int yMd = Integer.valueOf(yyyyMMddHHmmss.substring(0, 8));
//				boolean inBig = false;
//				for(Long[] bigSeq:period.inSeqbigtimes){
//					if (bigSeq[0] <= yMd
//							&& yMd <= bigSeq[1]) {
//						inBig = true;
//						break;
//					}
//				}
//				if (!inBig) 
//					return false;
//				for(Long[]small : period.inSeqSmalltimes){
//					if (small[0]<=hm && hm<=small[1]) 
//						return true;
//				}
//				return false;
//			} else if(TimePeriod.TYPE_DAY.equalsIgnoreCase(period.type)){
//				for(Long[]small : period.inSeqSmalltimes){
//					if (small[0]<=hm && hm<=small[1]) 
//						return true;
//				}
//				return false;
//			} else {
//				SimpleDateFormat format = getDateFormat();
//				Date date = format.parse(yyyyMMddHHmmss);
//				Calendar cal = getCalendar();
//				cal.setTime(date);
//				if(TimePeriod.TYPE_WEEK.equalsIgnoreCase(period.type)){
//					int weekday = cal.get(Calendar.DAY_OF_WEEK);
//					weekday = weekday-1;
//					if (0 == weekday) 
//						weekday = 7;
//					boolean inBig = false;
//					if(null != period.inSeqbigtimes)
//					for(Long[] bigSeq:period.inSeqbigtimes){
//						if (bigSeq[0] <= weekday && weekday <= bigSeq[1]) {
//							inBig = true;
//							break;
//						}
//					}
//					if (!inBig) {
//						if(null != period.inbigtimes)
//						for (long bigi : period.inbigtimes) {
//							if(bigi == weekday){
//								inBig = true;
//								break;
//							}
//						}
//					}
//					if (!inBig) 
//						return false;
//					for(Long[]small : period.inSeqSmalltimes){
//						if (small[0]<=hm && hm<=small[1]) 
//							return true;
//					}
//					return false;
//				} else if(TimePeriod.TYPE_MONTH.equalsIgnoreCase(period.type)){
//					int day = cal.get(Calendar.DAY_OF_MONTH);
//					boolean inBig = false;
//					if(null != period.inSeqbigtimes)
//					for(Long[] bigSeq:period.inSeqbigtimes){
//						if (bigSeq[0] <= day && day <= bigSeq[1]) {
//							inBig = true;
//							break;
//						}
//					}
//					if (!inBig) {
//						if(null != period.inbigtimes)
//						for (long bigi : period.inbigtimes) {
//							if(bigi == day){
//								inBig = true;
//								break;
//							}
//						}
//					}
//					if (!inBig) 
//						return false;
//					for(Long[]small : period.inSeqSmalltimes){
//						if (small[0]<=hm && hm<=small[1]) 
//							return true;
//					}
//					return false;
//				}
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		} 
//			
//		return false;
	}
	
	public boolean longtimeIntimes(long utc){
		if(null == period || null == period.type) {
            return false;
        }
		if (null == period.inSeqSmalltimes) {
            return false;
        }
		return dateIntimes(new Date(utc));
	}
	boolean isNumber(String str) {
        if (!ObjectUtils.isNullOrEmpty(str) && str.matches("[0-9]*.[0-9]{0,50}")) {
            return true;
        }
        return false;
    }
	public void addRule(Rule r){
		if(null == rules) {
            rules = new LinkedList<Rule>();
        }
		rules.add(r);
	}
	
//	private Calendar getCalendar(){
//		Calendar cal = callocal.get();
//		if (null == cal) {
//			cal = Calendar.getInstance();
//			callocal.set(cal);
//		}
//		return callocal.get();
//	}
	
	private SimpleDateFormat getDateFormat(){
		SimpleDateFormat format = formatlocal.get();
		if (null == format) {
			format = new SimpleDateFormat("yyyyMMddHHmmss");
			formatlocal.set(format);
		}
		return formatlocal.get();
	}
	
	private boolean intimeQuantum(String yyyyMMddHHmmss){
		if (null == yyyyMMddHHmmss
				||null == timeQuantum
				|| 0 == timeQuantum.length) {
            return false;
        }
		long msgTime=Long.valueOf(yyyyMMddHHmmss);
		for (String string : timeQuantum) {
			if (null !=string) {
				String []quantums = string.split("\\|",-1);
				if (ObjectUtils.isNullOrEmpty(quantums[0].trim()) 
						|| ObjectUtils.isNullOrEmpty(quantums[1].trim())) {
					continue;
				}
				long st = Long.valueOf(quantums[0]);
				long ed = Long.valueOf(quantums[1]);
				
				if (st<=msgTime && msgTime<=ed) {
					return true;
				}
			}
		}
		return false;
	}
	private boolean intimeQuantum(long utc){
		if (0 == utc
				||null == timeQuantum
				|| 0 == timeQuantum.length) {
            return false;
        }
		
		try {
			for (String string : timeQuantum) {
				if (null !=string) {
					String []quantums = string.split("\\|",-1);
					if (ObjectUtils.isNullOrEmpty(quantums[0].trim()) 
							|| ObjectUtils.isNullOrEmpty(quantums[1].trim())) {
						continue;
					}
					long st = getDateFormat().parse(quantums[0]).getTime();
					long ed = getDateFormat().parse(quantums[1]).getTime();
					
					if (st<=utc && utc<=ed) {
						return true;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return false;
	}
	
	private boolean nowIntimeQuantum(){
		return intimeQuantum(System.currentTimeMillis());
	}
	
	@Override
	public String toString() {
		return "EleFence [id=" + id + ", name=" + name + ", type=" + type + ", status=" + status + ", operType="
				+ operType + ", timeQuantum=" + Arrays.toString(timeQuantum) + ", segments=" + segments + ", polygons="
				+ polygons + ", circles=" + circles + ", rules=" + rules + ", judge=" + judge + ", pointRange="
				+ pointRange + ", timesegs=" + timesegs + "]";
	}
	public static void main(String[] args) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		System.out.println("HOUR:"+cal.get(Calendar.HOUR_OF_DAY));
		System.out.println("MONTH:"+cal.get(Calendar.MONTH));
		System.out.println("WEEKDAY:"+(cal.get(Calendar.DAY_OF_WEEK)-1));
		System.out.println("DAY:"+cal.get(Calendar.DAY_OF_MONTH));
		System.out.println("MINUTE:"+cal.get(Calendar.MINUTE));
		System.out.println("YEAR:"+cal.get(Calendar.YEAR));
		String ds="20170909151022";
		System.out.println(ds.substring(0, 8));
		System.out.println(ds.substring(8, 12));
	}
}
