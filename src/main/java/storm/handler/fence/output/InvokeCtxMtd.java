package storm.handler.fence.output;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import storm.handler.fence.input.Rule;
import storm.handler.fence.input.StopAlarmRule;
import storm.system.DataKey;
import storm.system.SysDefine;
import ent.calc.util.ConfigUtils;

public class InvokeCtxMtd extends InvokeMtd implements Invoke {
	private static final ConfigUtils configUtils = ConfigUtils.getInstance();

	Map<String, List<Map<String, String>>> ctxData;//vid ,map data
	Map<String, Long> lastZeroSpeedTime;//vid time
	static int datsize=6;
	{
		ctxData=new java.util.concurrent.ConcurrentHashMap<String, List<Map<String, String>>>();
		lastZeroSpeedTime=new java.util.concurrent.ConcurrentHashMap<String,Long>();
		if(null != configUtils.sysDefine.getProperty("ctx.cache.no")) {
			datsize=Integer.valueOf(configUtils.sysDefine.getProperty("ctx.cache.no"));
		}
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
		String vid = dat.get(DataKey.VEHICLE_ID);
		if(rule instanceof StopAlarmRule) {
			return invoke(rule,dat,vid);
		}
		addData(vid,dat,datsize);
		return invoke(vid,ctxData, rule);
	}
	
	Object invoke(Rule rule,Map<String, String> dat,String vid){
		Map<String, Object> rst = null;
		if(rule instanceof StopAlarmRule){
			long now = System.currentTimeMillis();
			StopAlarmRule alarmRule=(StopAlarmRule)rule;
			boolean isalm = false;
			boolean iszero = isSpeedZero(dat);
			if (iszero) {
				long time = getStringDatetoLong(dat.get("2000"));
				if (-1 != time) {
					
					if (lastZeroSpeedTime.containsKey(vid)) {
						long lasttime = lastZeroSpeedTime.get(vid);
						if (0 == lasttime) {
							lasttime = time;
							lastZeroSpeedTime.put(vid, lasttime);
						}
						if (now -lasttime > alarmRule.stopTime*1000) {
							isalm =true;
						}
						
					} else {
						lastZeroSpeedTime.put(vid, time);
					}
				}
			} else {
				lastZeroSpeedTime.put(vid, 0L);
			}

			if (isalm) {
				rst = new TreeMap<String, Object>();
				rst.put(rule.getCode(), isalm);
				rst.put(SysDefine.CODE, rule.getCode());
			}
		}
		return rst;
	}
	
	Object invoke(String vid,Map<String, List<Map<String, String>>> datas,Rule rule){
		Map<String, Object> rst = null;
		
		return rst;
	}
	
	boolean continuousZero(String vid,Map<String, List<Map<String, String>>> ctxdatas){

		if (MapUtils.isEmpty(ctxdatas)) {
			return false;
		}
		
		List<Map<String, String>> datas=ctxdatas.get(vid);
		
		return continuousZero(datas);
	}
	
	private boolean isSpeedZero(Map<String, String> data){
		if (null == data) {
			return false;
		}
		String speed = data.get("2201");
		if("0".equals(speed)) {
			return true;
		}
		return false;
	}
	boolean continuousZero(List<Map<String, String>> datas){

		if (CollectionUtils.isEmpty(datas) || datas.size()<datsize) {
			return false;
		}
		
		boolean allzero=true;
		for(Map<String, String> data : datas){
			String speed = data.get("2201");
			if (!StringUtils.isEmpty(speed)){
				
				if(!"0".equals(speed)){
					allzero = false;
					break;
				}
			}
		}
		return allzero;
	}
	public Map<String, List<Map<String, String>>> ctxDat(){
		return ctxData;
	}
	
	public void addData(String key,Map<String,String> data){
		if (!StringUtils.isEmpty(key)) {
			List<Map<String, String>>datas = ctxData.get(key);
			if (null==datas) {
				datas = new LinkedList<Map<String, String>>();
			}
			datas.add(data);
			ctxData.put(key, datas);
		}
	}
	
	public void addData(String key,Map<String,String> data,int datsize){
		if (!StringUtils.isEmpty(key)) {
			List<Map<String, String>>datas = ctxData.get(key);
			if (null==datas) {
				datas = new LinkedList<Map<String, String>>();
			}
			if (datas.size()>datsize) {
				datas.remove(0);
			}
			datas.add(data);
			ctxData.put(key, datas);
		}
	}
	
	private long getStringDatetoLong(String date){
		if(null == date || "".equals(date.trim())) {
			return -1;
		}
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
        try {
        	Date d = sdf.parse(date);
            return d.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return -1;
    }
	
}
