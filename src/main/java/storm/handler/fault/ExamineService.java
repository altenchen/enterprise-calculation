package storm.handler.fault;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.alibaba.fastjson.JSON;

import storm.dto.fault.AlarmMessage;
import storm.dto.fault.FaultMessage;
import storm.dto.fault.FaultRule;
import storm.util.dbconn.Conn;

/**
 * 检查服务
 */
public class ExamineService {

	Map<String, Map<String,AlarmMessage>>vidAlarmMsgs;//<vid,<alarmRruleid,msg>>
	Map<String, Map<String,FaultMessage>>vidFaultMsgs;//<vid,<faultRuleid,msg>>
	Collection<FaultRule> faultRules;
	Conn conn;
	{
		vidAlarmMsgs = new HashMap<String, Map<String,AlarmMessage>>();
		vidFaultMsgs = new HashMap<String, Map<String,FaultMessage>>();
		conn = new Conn();
		initRules();
	}
	private void initRules(){
		/**
		 * 初始化故障规则
		 */
		faultRules = conn.getFaultAndDepends();
	}
	
	public void reflushRules(){
		initRules();
	}
	
	/**
	 * 返回故障结果的JSON
	 * @param alarmMsg
	 * @return
	 */
	private List<String> getFaultSend(Map<String, String>alarmMsg){
		if (null == faultRules) 
			return null;
		List<String>sendMsg = null;
		String vid = alarmMsg.get("VID");
		long utctime = Long.valueOf(alarmMsg.get("UTC_TIME"));
		Map<String,FaultMessage> faultRuleResult = vidFaultMsgs.get(vid);
		if (null == faultRuleResult) 
			faultRuleResult = new TreeMap<String,FaultMessage>();
		sendMsg = new LinkedList<String>();
		for (FaultRule faultRule : faultRules) {
			if (null == faultRule || null == faultRule.id) 
				continue;
			
			FaultMessage message = faultRuleResult.get(faultRule.id);
			boolean success = faultRule.triggerSuccess(vidAlarmMsgs.get(vid));
			if (success) {
				if (null == message) {
					message = new FaultMessage(utctime, faultRule.id);
					message.triggerCounter = 1;
					message.status = 1;
				} else {
					message.triggerCounter = message.triggerCounter+1;
					message.status = 2;
				}
				if(null == message.vid)
					message.vid = vid;
					
				message.time = utctime;
				message.sustainTime = message.time-message.startTime;
				int level = faultRule.getLevel(message.triggerCounter, message.sustainTime);
				message.risk=level;
				faultRuleResult.put(faultRule.id, message);
			}else {
				if(null != message){
					
					message.endTime = utctime;
					message.status=3;
					faultRuleResult.remove(faultRule.id);
				}
			}
			if (null != message && -999 != message.risk) {
				String json = JSON.toJSONString(message);//jsonMsg(message);
				sendMsg.add(json);
			}
		}
		vidFaultMsgs.put(vid, faultRuleResult);
		return sendMsg;
	}
	
	private String jsonMsg(FaultMessage message){
		if(null == message)
			return null;
		if (-999 != message.risk) {
			Map<String, Object> map = new TreeMap<String,Object>();
			map.put("MSG_ID", message.msgId);
			map.put("FAULT_ID", message.faultRuleId);
			map.put("VID", message.vid);
			map.put("STATUS", message.status);
			map.put("RISK", message.risk);
			map.put("START_TIME", message.startTime);
			map.put("END_TIME", message.endTime);
			map.put("TIME", message.time);
			map.put("SUSTAIN_TIME", message.sustainTime);
			
			return JSON.toJSONString(map);
		}
		
		return null;
	}
	public List<String> handler(Map<String, String>alarmMsg){
		if (null == alarmMsg || alarmMsg.size()<5) 
			return null;
		
		String vid = alarmMsg.get("VID");
		String alarmRuleId = alarmMsg.get("CONST_ID");
		String status = alarmMsg.get("STATUS");
		String alarmId = alarmMsg.get("ALARM_ID");
//		String time = alarmMsg.get("TIME");
		long utctime = Long.valueOf(alarmMsg.get("UTC_TIME"));
//		long now = System.currentTimeMillis();
		
		Map<String,AlarmMessage> alarmRuleResult = vidAlarmMsgs.get(vid);
		if (null == alarmRuleResult) 
			alarmRuleResult = new TreeMap<String,AlarmMessage>();
		if ("3".equals(status)) {//代表告警结束
			alarmRuleResult.remove(alarmRuleId);
		} else {
			
			AlarmMessage msg = alarmRuleResult.get(alarmRuleId);
			if (null == msg) 
				msg = new AlarmMessage(alarmId,utctime, alarmRuleId);
			
			if ("1".equals(status)) {//告警开始
				msg.startTime = utctime;
				msg.initZero();
			} else if ("2".equals(status)) {//告警持续
				msg.time = utctime;
				msg.sustainTime = msg.time-msg.startTime;
			}
			msg.addCounter();
			alarmRuleResult.put(alarmRuleId, msg);
		}
		vidAlarmMsgs.put(vid, alarmRuleResult);
		
		return getFaultSend(alarmMsg);
	}
	public static void main(String[] args) {
		ExamineService service = new ExamineService();
		Map<String,String> sendFault = new TreeMap<String,String>();
        sendFault.put("VID", "003c45bd-692f-49f2-9f03-a558e8a9f6f5");
        sendFault.put("ALARM_ID", "003c45bd-692f-49f2-9f03-a558e8a9f6f5_20170926162628_402871815ebbd134015ebbe8885e0008");
        sendFault.put("STATUS", "1");
        sendFault.put("TIME", "20170926162458");
        sendFault.put("CONST_ID", "402871815ebbd134015ebbe8885e0008");
        sendFault.put("UTC_TIME", "1506414298000");
        
		List<String> reStrings=service.handler(sendFault);
		
		Map<String,String> alarmmsgs = new TreeMap<String,String>();
		alarmmsgs.put("VID", "003c45bd-692f-49f2-9f03-a558e8a9f6f5");
		alarmmsgs.put("ALARM_ID", "003c45bd-692f-49f2-9f03-a558e8a9f6f5_20170926162628_402871815ebbd134015ebbe8885e0008");
		alarmmsgs.put("STATUS", "2");
		alarmmsgs.put("TIME", "20170926162638");
		alarmmsgs.put("CONST_ID", "402871815ebbd134015ebbe8885e0008");
		alarmmsgs.put("UTC_TIME", "1506414398000");
		
		List<String> res=service.handler(alarmmsgs);
		
		alarmmsgs = new TreeMap<String,String>();
		alarmmsgs.put("VID", "003c45bd-692f-49f2-9f03-a558e8a9f6f5");
		alarmmsgs.put("ALARM_ID", "003c45bd-692f-49f2-9f03-a558e8a9f6f5_20170926162628_402871815ebbd134015ebbe8885e0008");
		alarmmsgs.put("STATUS", "2");
		alarmmsgs.put("TIME", "20170926162648");
		alarmmsgs.put("CONST_ID", "402871815ebbd134015ebbe8885e0008");
		alarmmsgs.put("UTC_TIME", "1506414408000");
		
		res=service.handler(alarmmsgs);
		
		alarmmsgs.put("TIME", "20170926162658");
		alarmmsgs.put("UTC_TIME", "1506414418000");
		res=service.handler(alarmmsgs);
		
		alarmmsgs.put("TIME", "20170926162708");
		alarmmsgs.put("UTC_TIME", "1506414428000");
		res=service.handler(alarmmsgs);
		
		alarmmsgs.put("TIME", "20170926162718");
		alarmmsgs.put("UTC_TIME", "1506414438000");
		res=service.handler(alarmmsgs);
		
		alarmmsgs.put("TIME", "20170926162728");
		alarmmsgs.put("UTC_TIME", "1506414448000");
		res=service.handler(alarmmsgs);
		
		alarmmsgs.put("TIME", "20170926162738");
		alarmmsgs.put("UTC_TIME", "1506414458000");
		res=service.handler(alarmmsgs);
		
		alarmmsgs.put("TIME", "20170926162748");
		alarmmsgs.put("UTC_TIME", "1506414468000");
		res=service.handler(alarmmsgs);
		
		alarmmsgs.put("TIME", "20170926162758");
		alarmmsgs.put("UTC_TIME", "1506414478000");
		res=service.handler(alarmmsgs);
		
		alarmmsgs.put("TIME", "20170926162808");
		alarmmsgs.put("UTC_TIME", "1506414488000");
		res=service.handler(alarmmsgs);
		
		alarmmsgs.put("TIME", "20170926162818");
		alarmmsgs.put("UTC_TIME", "1506414498000");
		res=service.handler(alarmmsgs);
		
		alarmmsgs.put("TIME", "20170926162828");
		alarmmsgs.put("UTC_TIME", "1506414508000");
		res=service.handler(alarmmsgs);
		
		alarmmsgs.put("TIME", "20170926162838");
		alarmmsgs.put("UTC_TIME", "1506414518000");
		res=service.handler(alarmmsgs);
		
	}
}
