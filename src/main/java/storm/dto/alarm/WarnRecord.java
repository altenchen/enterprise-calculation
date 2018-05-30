package storm.dto.alarm;

import storm.util.ObjectUtils;

public class WarnRecord {

	String id;
	String warnId;
	
	boolean isTrigger;//是否已经满足了触发次数或者时间
	long triggerTimes;//触发规则的持续时间
	int triggerCounts;//触发规则的持续次数
	
	boolean isVanish;//是否已经满足了结束的次数或者时间
	long vanishTimes;//不满足条件持续的时间
	int vanishCounts;//不满足条件持续次数
	
	transient int status;//0代表 预警 事件不在持续或者结束，1代表事件持续中
	
	String terminalSTime;//终端开始时间
	String terminalETime;//终端结束时间
	long sTime;//告警第一次开始时开始UTC时间
	long eTime;//告警第一次结束时开始UTC时间
	String realValue;//终端上传上来的值
	
	
	public WarnRecord(String warnId) {
		super();
		this.warnId = warnId;
	}

	public boolean isHasEnd(){
		if (ObjectUtils.isNullOrEmpty(terminalETime)) {
			return true;
		}
		return false;
	}
	
	public Object getSendMsg(){
		
		flush();
		return null;
	}
	
	public void count(String terTime,int endst){
		if (0 == status+endst) {
			//需要加一个判定,如开始没有预警产生 或者预警已经结束无需调用（结束次数）
			addVanishCounts();
		} else if (2 == status+endst) {
			//触发次数
			addTriggerCount();
		} else {
			
			if (!ObjectUtils.isNullOrEmpty(terTime)) {
				
				if (0 == endst) {
					warnVanishAction(terTime);
				} else if (1 == endst) {
					warnTriggerAction(terTime);
				}
			}
		}
	}
	
	private void warnTriggerAction(String terTime){
		setTerminalSTime(terTime);
		addTriggerCount();
		setStatus(1);
		//清空预警结束的状态
		flushVanish();
	}
	
	private void warnVanishAction(String terTime){
		setTerminalETime(terTime);
		addVanishCounts();
		setStatus(0);
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean isTrigger() {
		return isTrigger;
	}

	public void setTrigger(boolean isTrigger) {
		this.isTrigger = isTrigger;
	}

	public boolean isVanish() {
		return isVanish;
	}

	public void setVanish(boolean isVanish) {
		this.isVanish = isVanish;
	}

	public String getRealValue() {
		return realValue;
	}

	public void setRealValue(String realValue) {
		this.realValue = realValue;
	}

	public String getTerminalSTime() {
		return terminalSTime;
	}

	public String getTerminalETime() {
		return terminalETime;
	}

	public void set(String terTime,int es){//es=0代表没有或结束，es=1代表开始或者产生code对应的事件或错误
		if (0 == status+es) {
			return;
		}
		if (2 == status+es) {
			addTriggerCount();
		} else {
			if (ObjectUtils.isNullOrEmpty(terTime)) {
				return ;
			}
			if (0 == es) {
				setTerminalETime(terTime);
				setStatus(0);
			} else if (1 == es) {
				flush();
				setTerminalSTime(terTime);
				addTriggerCount();
				setStatus(1);
			}
		}
	}
	
	private void flushTrigger(){
		
		isTrigger = false;//是否已经满足了触发次数或者时间
		triggerTimes = 0L;//触发规则的持续时间
		triggerCounts = 0;//触发规则的持续次数
		terminalSTime = null;
	}
	
	private void flushVanish(){

		isVanish = false;//是否已经满足了结束的次数或者时间
		vanishTimes = 0L;//不满足条件持续的时间
		vanishCounts = 0;//不满足条件持续次数
		terminalETime = null;
	}
	
	public void flush(){
		
		flushTrigger();
		flushVanish();
		
		status = 0;//0代表 code 事件不在持续或者结束，1代表code事件持续中
		
		//终端时间
		realValue = null;
		
		sTime = 0L;//告警开始时间
		eTime = 0L;//告警结束开始时间
	}

	private void setTerminalSTime(String time){//yyyyMMddhhmmss
		if (! ObjectUtils.isNullOrEmpty(time)){
			if (null == terminalSTime) {
				terminalETime = null;
				terminalSTime = time;
			}
		}
	}
	
	private void setTerminalETime(String time){
		if (! ObjectUtils.isNullOrEmpty(time)) {
			
			terminalETime = time;
		}
	}

	private void addTriggerCount(){
		triggerCounts++;
	}
	
	private void addVanishCounts(){
		vanishCounts++;
	}

	private void setStatus(int status) {
		this.status = status;
	}
}
