package storm.dto.fault;

import java.io.Serializable;

/**
 * @author waz
 * 故障规则处理
 */
public abstract class AbstractDef implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 15200000021L;

	int level;//级别
	int mintimes;//最少次数
	int maxtimes;//最多次数
	int minduration;//最少持续时间 单位小时
	int maxduration;//最多持续时间 单位小时
	private boolean first;
	AbstractDef nextDef;
	public AbstractDef(int level, int mintimes, int maxtimes, int minduration, int maxduration) {
		this(level,mintimes,maxtimes,minduration,maxduration,false);
	}
	
	public AbstractDef(int level, int mintimes, int maxtimes, int minduration, int maxduration,boolean first) {
		super();
		this.level = level;
		this.mintimes = mintimes;
		this.maxtimes = maxtimes;
		this.minduration = minduration;
		this.maxduration = maxduration;
		this.first = first;
	}

	abstract Object calcul(Object ...objs);
	/**
	 * 
	 * @param times 次数
	 * @param duration 持续时间
	 * @return
	 */
	int cal(int times,long duration){
		if (! isFirst()) 
			return -999;
		if (times<0 || duration<0) 
			return -999;
		if ((mintimes <0 || maxtimes<0)
				&& (minduration <0 || maxduration<0) ) 
			return -999;
		
		int risk = caltime(times);
		if(-999 != risk)
			return risk;
		risk = caldur(duration);
		return risk;
	}
	
	private int caltime(int times){
		if (mintimes <= times && times <= maxtimes) 
			return level;
		if (null != nextDef) 
			return nextDef.caltime(times);
		return -999;
	}
	
	private int caldur(long duration){
		if(minduration*3600000 <= duration && duration < maxduration*3600000)
			return level;
		
		if (null != nextDef) 
			return nextDef.caldur(duration);
		return -999;
	}
	
	public void setNextDef(AbstractDef nextDef) {
		this.nextDef = nextDef;
	}

	private boolean isFirst() {
		return first;
	}

	void setFirst(boolean first) {
		this.first = first;
	}

	public void addNextDef(AbstractDef next){
		if (null == next || next == this) 
			return;
		next.setFirst(false);
		if(null == nextDef)
			setNextDef(next);
		else 
			nextDef.addNextDef(next);
	}
}
