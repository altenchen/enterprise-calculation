package storm.dto.fault;

import java.io.Serializable;
import java.util.List;

import storm.dto.TimePeriod;

public class FaultAlarmRule implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 12315011111L;

	public String id;
	public String name;
	public int type;
	public int risk;
	public String leftField1;
	public String leftOper1;
	public String leftField2;
	public String leftOper2;
	public float rightVal1;
	public float rightVal2;
	/**
	 * 临时字段 start
	 */
	public transient String timeString;
	public transient String dependId;
	/**
	 * 临时字段 end
	 */
	List<FaultAlarmRule> dependRules;
	public TimePeriod period;//生效时间段
	public boolean isalive;
	public long lastalivetime;
	{
		isalive = false;
		lastalivetime = 0L;
	}
	public boolean nowIsalive(){
		if (null == period) 
			return true;
		
		long now = System.currentTimeMillis();
		if (now - lastalivetime > 300000) {//每5分钟判断是否处于激活状态
			isalive = period.nowIntimes();
			lastalivetime = now;
		} 
		return isalive;
	}
	public void bulid(){
		if(null != timeString){
			
		}
		if (null != dependId) {
			
		}
	}
	@Override
	public String toString() {
		return "FaultAlarmRule [id=" + id + ", name=" + name + ", type=" + type + ", risk=" + risk + ", leftField1="
				+ leftField1 + ", leftOper1=" + leftOper1 + ", leftField2=" + leftField2 + ", leftOper2=" + leftOper2
				+ ", rightVal1=" + rightVal1 + ", rightVal2=" + rightVal2 + ", dependRules=" + dependRules + ", period="
				+ period + ", isalive=" + isalive + ", lastalivetime=" + lastalivetime + "]";
	}
}
