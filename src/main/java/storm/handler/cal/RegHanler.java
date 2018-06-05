package storm.handler.cal;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.sun.jersey.core.util.Base64;

import storm.protocol.SUBMIT_LOGIN;
import storm.system.ProtocolItem;
import storm.system.SysDefine;
import storm.util.ObjectUtils;

class RegHanler {

	private Map<String, Boolean> regVids;//vid ,isreg
	{
		regVids = new HashMap<String, Boolean>();
	}
	
	Map<String,Object> regHandler(Map<String, String> msg){
		if (null == msg || msg.size()<1) 
			return null;
		
		String type = msg.get(ProtocolItem.REG_TYPE);
        String time = msg.get(SysDefine.TIME);

        try {
			if ("1".equals(type)) {//车辆上线
			    String vid = msg.get(SysDefine.VID);
			    if (vid==null || vid.equals("")){
			        System.out.println("Login Error VID: ");
			        return null;
			    }

			    String iccid = null;
			    String icc = msg.get(ProtocolItem.ICCID);
			    if (!ObjectUtils.isNullOrEmpty(icc)) {
					
			    	iccid = new String(Base64.decode(icc),"GBK");
				}
			    if (iccid==null || "".equals(iccid.trim())) {
			    	icc = msg.get(SUBMIT_LOGIN.ICCID_ITEM);
				    if (!ObjectUtils.isNullOrEmpty(icc)) {
				    	iccid = new String(Base64.decode(icc),"GBK");
					}
			    }
			    if (iccid==null || "".equals(iccid.trim())) 
			    	iccid = "";

			    Map<String, Object> esmap = new TreeMap<String, Object>();
			    esmap.put(EsField.vid, vid);
			    esmap.put(EsField.terminalTime, toTimeString(time));
			    if (! "".equals(iccid)){
			    	iccid = iccid.trim();
			    	int end = iccid.indexOf("\\0",0);
			    	if (end != -1) 
			    		iccid = iccid.substring(0, end);
			    	if (! "".equals(iccid))
			    		esmap.put(EsField.iccid, iccid);
			    } 
					
			    if (regVids.containsKey(vid)) {
			    	esmap.put(EsField.carStatus, 1);
				} else {
					
					if ("0".equals(msg.get(ProtocolItem.REG_STATUS))) {
						regVids.put(vid, true);
						esmap.put(EsField.monitor, 1);
						esmap.put(EsField.status, 0);
						esmap.put(EsField.carStatus, 1);
					}
				}
			    return esmap;
			}else if ("2".equals(type)) {//车辆离线
				String vid = msg.get(SysDefine.VID);
			    if (vid==null || vid.equals("")){
			        System.out.println("Login Error VID: ");
			        return null;
			    }

			    Map<String, Object> esmap = new TreeMap<String, Object>();
			    regVids.put(vid, false);
				esmap.put(EsField.vid, vid);
				esmap.put(EsField.terminalTime, toTimeString(time));
				esmap.put(EsField.carStatus, 0);
				esmap.put(EsField.onlineStatus, 0);
				//esmap.put(EsField.monitor, 1);
			    return esmap;
			   
			}else if ("0".equals(type)){
			    /*try {
			        String vid = msg.get(SysDefine.VID);
			        if (vid==null || "".equals(vid.trim()) || null == time || "".equals(time.trim())){
			            System.out.println("Login Error VID: " + vid);
			            return null;
			        }

			        StringBuffer sql = new StringBuffer("SELECT VIN," +
			                "LICENSE_PLATE," +
			                "SIM_CARD," +
			                "FACTORY_DATE," +
			                "IS_SMS_ALARM," +
			                "RESERVE1," +
			                "RESERVE2," +
			                "RESERVE3," +
			                "ONLINED," +
			                "DRIVE_MODE," +
			                "UUID," +
			                "FIRST_REG," +
			                "BOUNDCHECK," +
			                "UPDATE_TIME," +
			                "VEH_STATUS," +
			                "ALARM_STATUS," +
			                "ENTRY_DATE," +
			                "OFFICIAL_OPERATION_DATE," +
			                "SUM_SINGLE_KM," +
			                "KM_UPDATE_DATA," +
			                "START_MILEAGE," +
			                "OFFICIAL_OPERATION_SMILEA," +
			                "START_DATE," +
			                "ONLINE_TOTAL_DAY," +
			                "TOTAL_BEFORE_OFFICIAL_DAY," +
			                "ICCID," +
			                "ID," +
			                "RULE_ID," +
			                "USE_UINT_ID," +
			                "SYS_STORE_POINT_ID," +
			                "MANU_UNIT_ID," +
			                "VEH_MODEL_ID," +
			                "TERM_UNIT_ID," +
			                "TERM_MODEL_ID," +
			                "VEH_TYPE_ID," +
			                "SYS_DIVISION_ID," +
			                "OWNER_ID," +
			                "CONTACTOR_ID," +
			                "LAST_OPER_ID FROM SYS_VEHICLE V WHERE V.UUID = '" + vid + "'");
			        Object[] constObjs = C3P0Utils.QueryObject(sql.toString(), null);
			        if (constObjs==null || constObjs.length<39){
			            System.out.println("Not find this vehicle UUID: " + vid);
			            return;
			        }

			        if ("1".equals(constObjs[8].toString())){
			            System.out.println("This vehicle had onlined: " + vid);
			            return;
			        }

			        sql = new StringBuffer();
			        if (SysGlobal.database_flag.equals(SysGlobal.database_mysql)) {
			            sql.append("UPDATE SYS_VEHICLE V SET ONLINED=1, FIRST_REG='").append(DateUtil.converseStr(time)).append("' ");
			            sql.append("WHERE V.UUID = '" + vid + "'");
			        } else {
			            sql.append("UPDATE SYS_VEHICLE V SET ONLINED=1, FIRST_REG=TO_DATE('");
			            sql.append(DateUtil.converseStr(time));
			            sql.append("', 'yyyy-mm-dd hh24:mi:ss') WHERE V.UUID = '");
			            sql.append(vid+"'");
			        }
			        C3P0Utils.UpdateSQL(sql.toString());

			        String buf = "ID=" + constObjs[26].toString() +
			                "&VIN=" + (constObjs[0].toString() != null ? constObjs[0].toString() : "") +
			                "&LICENSE_PLATE=" + (constObjs[1].toString() != null ? constObjs[1].toString() : "") +
			                "&MANU_UNIT_ID=" + (constObjs[30].toString() != null ? constObjs[30].toString() : "") +
			                "&SIM_CARD=" + (constObjs[2].toString() != null ? constObjs[2].toString() : "") +
			                "&FACTORY_DATE=" + (constObjs[3].toString() != null ? DateUtil.converseStr2(constObjs[3].toString()) : "") +
			                "&OFFICIAL_OPERATION_DATE=" + (constObjs[17].toString() != null ? DateUtil.converseStr2(constObjs[17].toString()) : "") +
			                "&IS_SMS_ALARM=" + (constObjs[4].toString() != null ? constObjs[4].toString() : "");

			        String VehBeforeUpdate = buf+"&ONLINED=0";
			        String VehAfterUpdate = buf+"&ONLINED=1";

			        buf = "&DRIVE_MODE=" + (constObjs[9].toString() != null ? constObjs[9].toString() : "") +
			                "&UUID=" + (constObjs[10].toString() != null ? constObjs[10].toString() : "") +
			                "&BOUNDCHECK=" + (constObjs[12].toString() != null ? constObjs[12].toString() : "") +
			                "&UPDATE_TIME=" + (constObjs[13].toString() != null ? DateUtil.converseStr2(constObjs[13].toString()) : "") +
			                "&VEH_STATUS=" + (constObjs[14].toString() != null ? constObjs[14].toString() : "") +
			                "&ALARM_STATUS=" + (constObjs[15].toString() != null ? constObjs[15].toString() : "") +
			                "&RULE_ID=" + (constObjs[27].toString() != null ? constObjs[27].toString() : "") +
			                "&USE_UINT_ID=" + (constObjs[28].toString() != null ? constObjs[28].toString() : "") +
			                "&VEH_MODEL_ID=" + (constObjs[31].toString() != null ? constObjs[31].toString() : "") +
			                "&SYS_DIVISION_ID=" + (constObjs[35].toString() != null ? constObjs[35].toString() : "") +
			                "&LAST_OPER_ID=" + (constObjs[38].toString() != null ? constObjs[38].toString() : "") +
			                "&VEH_TYPE_ID=" + (constObjs[34].toString() != null ? constObjs[34].toString() : "") +
			                "&TERM_MODEL_ID=" + (constObjs[31].toString() != null ? constObjs[31].toString() : "") +
			                "&TERM_UNIT_ID=" + (constObjs[32].toString() != null ? constObjs[32].toString() : "") +
			                "&SYS_STORE_POINT_ID=" + (constObjs[29].toString() != null ? constObjs[29].toString() : "") +
			                "&OWNER_ID=" + (constObjs[36].toString() != null ? constObjs[36].toString() : "");
			        VehBeforeUpdate += buf + "&FIRST_REG=" + "&CONTACTOR_ID=" + (constObjs[37].toString() != null ? constObjs[37].toString() : "");
			        VehAfterUpdate += buf + "&FIRST_REG=" + time + "&CONTACTOR_ID=" + (constObjs[37].toString() != null ? constObjs[37].toString() : "");

			        String KafkaParam = KafkaParam("SYS_VEHICLE", "2", VehBeforeUpdate, VehAfterUpdate);
			        KeyedMessage<String, String> message = new KeyedMessage<String, String>(SysGlobal.kafka_vehchange_topic, "VEH_CHANGE", KafkaParam);
			        SendKafkaData(message);

			        System.out.println("Register------------ VehUuid: " + vid + "  TIME:" + time);
			    } catch (Exception e) {
			        System.out.println(e.getMessage());
			    }*/
			}else if ("3".equals(type) || "4".equals(type)){
				/*String platId = msg.get(ProtocolItem.PLAT_ID);
			    if ((platId==null || "".equals(platId)) || (time==null || "".equals(time))){
			        System.out.println("Error PLATID: " + platId + "  TIME:" + time);
			        return null;
			    }

			    String seqId = msg.get(ProtocolItem.SEQ_ID);
			    String status = msg.get(ProtocolItem.REG_STATUS);
			    String username = msg.get(ProtocolItem.USERNAME);
			    String password = msg.get(ProtocolItem.PASSWORD);

			    StringBuffer sql = new StringBuffer();
			    if (type.equals("3")) {
			        sql.append("INSERT INTO SYS_PLATFORM_EVENT(ID, TYPE, TIME, PID, SEQID, STATUS, DESCRIPTION) VALUES('");
			        sql.append(UUID.randomUUID()).append("',");
			        if (SysGlobal.database_flag.equals(SysGlobal.database_mysql)) {
			            sql.append(0).append(", '");
			            sql.append(DateUtil.converseStr(time)).append("','");
			        } else {
			            sql.append(0).append(",TO_DATE('");
			            sql.append(DateUtil.converseStr(time)).append("', 'yyyy-mm-dd hh24:mi:ss'),'");
			        }
			        sql.append(platId).append("',");
			        sql.append(seqId).append(",");
			        sql.append(status).append(",'");
			        username = base64Decode(username);
			        sql.append(username).append(";");
			        password = base64Decode(password);
			        sql.append(password).append("')");

			        C3P0Utils.UpdateSQL(sql.toString());
			        if (status.equals("0")){
			            C3P0Utils.UpdateSQL("UPDATE SYS_PLATFORM_INFORMATION SET STATUS=1 WHERE ID='"+platId+"'");
			        }
			    } else {
			        if (seqId == null || "".equals(seqId)) {
			            sql.append("INSERT INTO SYS_PLATFORM_EVENT(ID, TYPE, TIME, PID) VALUES('");
			            sql.append(UUID.randomUUID()).append("',");
			            if (SysGlobal.database_flag.equals(SysGlobal.database_mysql)) {
			                sql.append(2).append(",'");
			                sql.append(DateUtil.converseStr(time)).append("','");
			            } else {
			                sql.append(2).append(",TO_DATE('");
			                sql.append(DateUtil.converseStr(time)).append("', 'yyyy-mm-dd hh24:mi:ss'),'");
			            }
			            sql.append(platId).append("')");

			            C3P0Utils.UpdateSQL(sql.toString());
			            C3P0Utils.UpdateSQL("UPDATE SYS_PLATFORM_INFORMATION SET STATUS=2 WHERE ID='" + platId + "'");
			        } else {
			            sql.append("INSERT INTO SYS_PLATFORM_EVENT(ID, TYPE, TIME, PID, SEQID, STATUS) VALUES('");
			            sql.append(UUID.randomUUID()).append("',");
			            if (SysGlobal.database_flag.equals(SysGlobal.database_mysql)) {
			                sql.append(1).append(",'");
			                sql.append(DateUtil.converseStr(time)).append("','");
			            } else {
			                sql.append(1).append(",TO_DATE('");
			                sql.append(DateUtil.converseStr(time)).append("', 'yyyy-mm-dd hh24:mi:ss'),'");
			            }
			            sql.append(platId).append("',");
			            sql.append(seqId).append(",");
			            sql.append(0).append(")");

			            C3P0Utils.UpdateSQL(sql.toString());
			            C3P0Utils.UpdateSQL("UPDATE SYS_PLATFORM_INFORMATION SET STATUS=0 WHERE ID='" + platId + "'");
			        }
			    }*/
			}else {
			    System.out.println("ERROR, TYPE is null OR others------------ TYPE: " + type);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	private String toTimeString(String intime){//yyyyMMddHHmmss
		if (null == intime || intime.length() != 14) {
			return null;
		}
		StringBuilder builder = new StringBuilder();
		builder.append(intime.substring(0, 4)).append("-")
		.append(intime.substring(4, 6)).append("-")
		.append(intime.substring(6, 8)).append(" ")
		.append(intime.substring(8, 10)).append(":")
		.append(intime.substring(10, 12)).append(":")
		.append(intime.substring(12));
		intime = null;
		return builder.toString();//yyyy-MM-dd HH:mm:ss
	}
}
