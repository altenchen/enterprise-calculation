package storm.dto.fault;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import storm.dto.TimePeriod;

/**
 * @author wza
 * 故障规则处理
 */
public class FaultRule implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 13210000001L;
	public static ThreadLocal<Integer> invokeCount = new ThreadLocal<Integer>();
	String name;
	public String id;
	String state;
	int isDisable;
	public String dependId;//暂时只有一层依赖，暂时只用于存储
	private List<FaultRule>dependFaultRules;//依赖其他的故障规则
	public List<FaultAlarmRule>dependAlarmRules;//依赖告警规则
	String selectItem;//数据项
	public String statisticalTime;//统计时间
	public String type;//故障类型
	private List<AbstractDef>defs;//级别定义链 不同情况下满足 1级 2级...
	private AbstractDef firstDef;//调用连开始类
//	private FaultRule previousFault;
	public TimePeriod period;//生效时间段
	public boolean isalive;
	public long lastalivetime;
	{
		isalive = false;
		lastalivetime = 0L;
		dependFaultRules = new LinkedList<FaultRule>();
		dependAlarmRules = new LinkedList<FaultAlarmRule>();
		defs = new LinkedList<AbstractDef>();
	}
	public void build(){
		resetDef();
	}
	public int getLevel(int times,long duration){
		if(null != firstDef)
			return firstDef.cal(times, duration);
		return -999;
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
	
	public void addDef(AbstractDef def){
		if (null != def) {
			if (! defs.contains(def)) 
				defs.add(def);
		}
	}
	
	private void resetDef(){
		if (null != defs) {
			firstDef = defs.get(0);
			firstDef.setFirst(true);
			for (int i = 1; i < defs.size(); i++) {
				AbstractDef def = defs.get(i);
				if (null != def) {
					
					firstDef.addNextDef(def);
				}
			}
		}
	}
	
	public void addFaultRule(FaultRule rule){
		if (null != rule && rule != this) {
			if (! dependFaultRules.contains(rule)) 
				dependFaultRules.add(rule);
		}
	}
	
	public void addFaultAlarmRule(FaultAlarmRule rule){
		if (null != rule) {
			if (! dependAlarmRules.contains(rule)) 
				dependAlarmRules.add(rule);
		}
	}
	
	public boolean triggerSuccess(Map<String,AlarmMessage> result){
		/**
		 * 定义如果循环引用则认为后面被引用的是无效的，直接返回错误
		 */
		int count = getInvoke();
		if (count>=2) {
			return false;
//			throw new RuntimeException("故障规则被循环调用");
		}
		setInvoke(count+1);
		boolean alarmAllYes = alarmTriggerSuccess(result);
		boolean faultAllYse = faultTriggerSuccess(result);
		
		setInvoke(0);
		return alarmAllYes && faultAllYse;
	}
	/**
	 * 按照指定告警规则产生的告警结果是否可以满足 触发故障
	 * @return 激活故障的结果，决定是否产生故障报文
	 */
	private boolean alarmTriggerSuccess(Map<String,AlarmMessage> result){
		/**<p>后面如果有生效的时间，
		 * 需要在生效时间内才有可能激活</p>
		 */
//		if (! nowIsalive()) 
//			return false;
		
		/**<p>由于没有定义详细触发规则，
		 * 目前只要产生告警，不管级别和时间，
		 * 只有有 所有指定规则的告警产生就认为是产生了故障，后面可能要优化
		 * </p>
		 */
		if (null == dependAlarmRules || dependAlarmRules.size() < 1) //代表不需要告警规则也可以触发成功
			return true;
		if(null == result || result.size()<1)
			return false;
		//简单处理，只要告警满足，不论级别代表都可以触发故障
		for (FaultAlarmRule rule : dependAlarmRules) {
			if (!result.containsKey(rule.id)) 
				return false;
			
		}
		return true;
	}
	
	/**
	 * <p>里面存在递归调用的风险
	 * 需要解决</p>
	 * @param result
	 * @return
	 */
	private boolean faultTriggerSuccess(Map<String,AlarmMessage> result){
		if(null == dependFaultRules || dependFaultRules.size() < 1)
			return true;
		/**
		 * <p>
		 * 现在简化处理，认为只要产生了故障，
		 * 有故障级别就可以，认为依赖的故障是产生的
		 * 后面根据业务场景可能需要更改
		 * </p>
		 */
		//int level = getLevel(times, duration);//故障的定义
		boolean isTg = true;//= -999 != level;//目前的业务定义是 依赖 和 告警都满足就可以，没有对告警的 级别有要求
		for (FaultRule rule : dependFaultRules) {
			boolean tsuccess = rule.triggerSuccess(result);
			isTg = isTg && tsuccess;
		}
		return isTg;
	}
	
	private boolean faultTrigger(Map<String,FaultMessage> result){
		if(null == dependFaultRules || dependFaultRules.size() < 1)
			return true;
		/**
		 * <p>
		 * 现在简化处理，认为只要产生了故障，
		 * 有故障级别就可以，认为依赖的故障是产生的
		 * 后面根据业务场景可能需要更改
		 * </p>
		 */
		FaultMessage message = result.get(id);
		int level = getLevel(message.triggerCounter, message.sustainTime);//故障的定义
		boolean isTg = -999 != level;//目前的业务定义是 依赖 就可以，没有故障 级别有要求
		for (FaultRule rule : dependFaultRules) {
			boolean tsuccess = rule.faultTrigger(result);
			isTg = isTg && tsuccess;
		}
		return isTg;
	}
	
	private int getInvoke(){
		int count = 0;
		if(null == invokeCount.get()
				|| 0 == invokeCount.get()){
			count = 1;
			invokeCount.set(count);
		}
		return invokeCount.get();
	}
	
	private void setInvoke(int count){
		invokeCount.set(count);
	}
	
	@Override
	public String toString() {
		return "FaultRule [name=" + name + ", id=" + id + ", state=" + state + ", isDisable=" + isDisable
				+ ", dependId=" + dependId + ", dependFaultRules=" + dependFaultRules + ", dependAlarmRules="
				+ dependAlarmRules + ", selectItem=" + selectItem + ", statisticalTime=" + statisticalTime + ", type="
				+ type + ", defs=" + defs + ", firstDef=" + firstDef + ", period=" + period + ", isalive=" + isalive
				+ ", lastalivetime=" + lastalivetime + "]";
	}
}
