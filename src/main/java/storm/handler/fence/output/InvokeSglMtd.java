package storm.handler.fence.output;


import java.util.Map;
import java.util.TreeMap;

import storm.handler.fence.input.AlarmRule;
import storm.handler.fence.input.AreaIOAlarmRule;
import storm.handler.fence.input.Rule;
import storm.handler.fence.input.SpeedAlarmRule;
import storm.system.SysDefine;
import storm.util.ObjectUtils;

public class InvokeSglMtd extends InvokeMtd implements Invoke {

	Map<String, String>data;
	
	public InvokeSglMtd(Map<String, String> data) {
		super();
		this.data = data;
	}

	public InvokeSglMtd() {
		super();
	}
	public void setData(Map<String, String> data) {
		this.data = data;
	}
	
	@Override
	public Result exe(Rule rule) {

		if (Rule.RuleType.VOID == rule.getType()) {
			//invoke(data);
			return Result.VOID;
		}
		Object object=invoke(data,rule);
		return Result.VALUE.setResultValue(object);
	}
	
	@Override
	public Result exe(Map<String, String> dat, Rule rule) {
		if (Rule.RuleType.VOID == rule.getType()) {
			//invoke(data);
			return Result.VOID;
		}
		Object object=invoke(dat,rule);
		return Result.VALUE.setResultValue(object);
	}

	Object invoke(Map<String, String> dat,Rule rule){
		Map<String, Object> rst = null;
		if(rule instanceof SpeedAlarmRule){
			SpeedAlarmRule alarmRule=(SpeedAlarmRule)rule;
			
			String speed = dat.get("2201");
			if (!ObjectUtils.isNullOrEmpty(speed)){
				double spd = Double.valueOf(speed);
			
				boolean isalm = false;
				if(alarmRule.speedType==AlarmRule.GT
						|| "0001".equals(alarmRule.getCode())){
					if(spd>alarmRule.speeds[1])
						isalm= true;
					
				} else if(alarmRule.speedType==AlarmRule.LT
						|| "0002".equals(alarmRule.getCode())){
					if(spd<alarmRule.speeds[0])
						isalm= true;
					
				} else if(alarmRule.speedType==AlarmRule.GLT
						|| "0001,0002".equals(alarmRule.getCode())){
					if(spd<alarmRule.speeds[0]
							||spd>alarmRule.speeds[1])
						isalm= true;
				}
				if (isalm) {
					rst = new TreeMap<String, Object>();
					rst.put(rule.getCode(), isalm);
					rst.put(SysDefine.CODE, rule.getCode());
				}
			}
		} else if(rule instanceof AreaIOAlarmRule){
			AreaIOAlarmRule alarmRule=(AreaIOAlarmRule)rule;
			rst = new TreeMap<String, Object>();
			rst.put(SysDefine.CODE, rule.getCode());
		} 
		return rst;
	}
	@Override
	public Result exe(Map<String, String> data, Rule rule,Map<String, Object>result) {
		
		return Result.VOID;
	}
}
