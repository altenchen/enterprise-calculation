package storm.handler;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import storm.dto.FaultCode;
import storm.dto.FaultRuleCode;
import storm.service.TimeFormatService;
import storm.system.ProtocolItem;
import storm.util.ConfigUtils;
import storm.util.ObjectUtils;
import storm.util.UUIDUtils;
import storm.util.dbconn.Conn;

public class FaultCodeHandler {

	static TimeFormatService timeformat;
	long lastflushtime;
	static long dbflushtime = 360000;//360秒
	static long offlinetime = 600000;//600秒
	static {
		timeformat = new TimeFormatService();
		if (null != ConfigUtils.sysDefine) {
			String dbflush = ConfigUtils.sysDefine.getProperty("db.cache.flushtime");
			if (!ObjectUtils.isNullOrEmpty(dbflush)) {
				dbflushtime = Long.parseLong(dbflush)*1000;
			}
			String off = ConfigUtils.sysDefine.getProperty("redis.offline.time");
			if (!ObjectUtils.isNullOrEmpty(off)) {
				offlinetime = Long.parseLong(off)*1000;
			}
		}
	}
    private Conn conn;
    private Collection<FaultRuleCode>rules;
    Map<String, Map<String,Map<String,Object>>>vidRuleMsgs;
    private Map<String, Long> lastTime;
    {
    	try {
    		vidRuleMsgs = new HashMap<String, Map<String,Map<String,Object>>>();
    		lastTime = new HashMap<String, Long>();
    		conn = new Conn();
        	lastflushtime = System.currentTimeMillis();
        	initRules();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    private void initRules(){
    	rules = conn.getFaultAlarmCodes();
    }
    
    private Collection<FaultRuleCode> getRules(){
    	long now = System.currentTimeMillis();
    	if (now - lastflushtime > dbflushtime) {
    		initRules();
    		lastflushtime = now;
		}
    	return rules;
    }
     
    public List<Map<String, Object>> handle(long now){
    	if (vidRuleMsgs.size() == 0) {
			return null;
		}
    	List<Map<String, Object>>notices = new LinkedList<Map<String, Object>>();
    	String noticetime = timeformat.toDateString(new Date(now));
    	List<String> needRemoves = new LinkedList<String>();
    	for (Map.Entry<String, Long> entry : lastTime.entrySet()) {
    		long last = entry.getValue();
			if (now - last > offlinetime) {
				String vid = entry.getKey();
				needRemoves.add(vid);
				Map<String,Map<String,Object>> ruleMsgs = vidRuleMsgs.get(vid);
				if (null != ruleMsgs) {
					for (Map.Entry<String,Map<String,Object>> ruleEntry : ruleMsgs.entrySet()) {
						
						Map<String, Object> msg = ruleEntry.getValue();
						if (null != msg) {
							msg.put("noticetime", noticetime);
							msg.put("status", 3);
							msg.put("etime", noticetime);
							
							notices.add(msg);
						}
					}
				}
			}
		}
    	
    	for (String vid : needRemoves) {
    		lastTime.remove(vid);
    		vidRuleMsgs.remove(vid);
		}
    	if (notices.size()>0) {
			return notices;
		}
		return null;
    }
	public List<Map<String, Object>> handle(Map<String, String>dat){
		if (ObjectUtils.isNullOrEmpty(dat)) {
			return null;
		}
		Collection<FaultRuleCode> rules = getRules();
		if (null == rules || rules.size() == 0) {
			return null;
		}
		String vid = dat.get(ProtocolItem.VID);
		String time = dat.get(ProtocolItem.TIME);
		if (ObjectUtils.isNullOrEmpty(vid)
				|| ObjectUtils.isNullOrEmpty(time)) {
			return null;
		}
		List<String>msgFcodes = new LinkedList<String>();
		//后续对应的字段需要在配置文件中配置
		String code2922 = dat.get("2922");//可充电储能故障码
		String code2805 = dat.get("2805");//驱动电机故障码
		String code2924 = dat.get("2924");//发动机故障码
		String code2809 = dat.get("2809");//其他故障(厂商扩展)
		//北汽故障码
		String codebq4510003 = dat.get("4510003");
		
		setMsgFcodes(code2922, msgFcodes);
		setMsgFcodes(code2805, msgFcodes);
		setMsgFcodes(code2924, msgFcodes);
		setMsgFcodes(code2809, msgFcodes);
		setMsgFcodes(codebq4510003, msgFcodes);
		
		if (msgFcodes.size() == 0) {
			return null;
		}
		List<Map<String, Object>>notices = new LinkedList<Map<String, Object>>();
		for (FaultRuleCode  ruleCode: rules) {
			List<Map<String, Object>>msgs = msgFault(dat, msgFcodes, ruleCode);
			if (null != msgs) {
				notices.addAll(msgs);
			}
		}
		if (notices.size() > 0) {
			return notices;
		}
		return null;
	}
	
	private void setMsgFcodes(String fcode,List<String>msgFcodes){
		if (null != fcode && !"".equals(fcode)) {
			String [] codes = fcode.split("\\|");
			for (String code : codes) {
				if (null != code && !msgFcodes.contains(code)) {
					msgFcodes.add(code);
				}
			}
		}
	}
	
	private List<Map<String, Object>> msgFault(Map<String, String>dat,List<String>msgFcodes,FaultRuleCode rule){
		List<Map<String, Object>>notices = new LinkedList<Map<String, Object>>();
		String vid = dat.get(ProtocolItem.VID);
		String time = dat.get(ProtocolItem.TIME);
		if (ObjectUtils.isNullOrEmpty(vid)
				|| ObjectUtils.isNullOrEmpty(time)) {
			return null;
		}
		String latit = dat.get(ProtocolItem.latitude);
		String longi = dat.get(ProtocolItem.longitude);
		String location = longi+","+latit;
		Date date = new Date();
		String noticetime = timeformat.toDateString(date);
		long last = date.getTime();
		Map<String,Map<String,Object>> ruleMsgs = vidRuleMsgs.get(vid);
		List<FaultCode> codes = rule.codes;
		for (FaultCode faultCode : codes) {
			String fcode = hexToDec(faultCode.code);
			if (msgFcodes.contains(fcode)) {
				lastTime.put(vid, last);
				boolean end = (faultCode.type ==0 );
				if (end) {
					if (null != ruleMsgs) {
						Map<String, Object> msg = ruleMsgs.get(rule.ruleId);
						if (null != msg) {
							msg.put("ruleId", faultCode.id);
							msg.put("noticetime", noticetime);
							msg.put("status", 3);
							msg.put("etime", time);
							msg.put("location", location);
							ruleMsgs.remove(rule.ruleId);
							notices.add(msg);
						}
						if (notices.size()>0) {
							return notices;
						}
						return null;
					}
				}
				boolean start = (1 == faultCode.type);
				if (start) {
					
					if (null == ruleMsgs) {
						ruleMsgs = new TreeMap<String,Map<String, Object>>();
					}
					Map<String, Object> msg = ruleMsgs.get(rule.ruleId);
					if (null == msg) {
						msg = newCodeMsg();
						msg.put("vid", vid);
						msg.put("status", 1);
						msg.put("stime", time);
						msg.put("level", faultCode.level);
					} else {
						if ((int)msg.get("level") == faultCode.level) {
							msg.put("status", 2);
						}else{
							msg.put("status", 1);
							msg.put("stime", time);
							msg.put("level", faultCode.level);
						}
					}
					msg.put("ruleId", faultCode.id);
					msg.put("faultCode", faultCode.code);
					msg.put("noticetime", noticetime);
					msg.put("location", location);
					//添加同通知消息
					if(1 == (int)msg.get("status"))
						notices.add(msg);
					//添加缓存
					ruleMsgs.put(rule.ruleId, msg);
					vidRuleMsgs.put(vid, ruleMsgs);
				}
			}
		}
		if (notices.size()>0) {
			return notices;
		}
		return null;
	}
	
	/**
	 * <p>
	 * 此方法是需要自己将10进制值转换为16进制后再进行判定
	 * 后续采用原始值字符串时候就不需要再这样处理
	 * </p>
	 * @param hex
	 * @return
	 */
	String hexToDec(String hex){
		if (null == hex || "".equals(hex.trim())) {
			return "-1";
		}
		if (hex.startsWith("0x") || hex.startsWith("0X")) {
			if (hex.length()>2) {
				hex = hex.substring(2);
				return ""+Long.parseLong(hex, 16);
			}
		} 
		return hex;
	}
	
	Map<String,Object> newCodeMsg(){
		Map<String,Object> msg = new TreeMap<String,Object>();
		msg.put("msgType", "FAULT_CODE_ALARM");
		msg.put("msgId", UUIDUtils.getUUID());
		return msg;
	}
	
	public static void main(String[] args) {
		FaultCodeHandler handler = new FaultCodeHandler();
		String res = handler.hexToDec("0x000010");
		System.out.println(res);
	}
}
