package storm.util.dbconn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import storm.dto.FaultCode;
import storm.dto.FaultRuleCode;
import storm.dto.fault.FaultAlarmRule;
import storm.dto.fault.FaultRule;
import storm.dto.fault.RiskDef;
import storm.dto.fence.EleFence;
import storm.handler.fence.input.AlarmRule;
import storm.handler.fence.input.Rule;
import storm.handler.fence.input.SpeedAlarmRule;
import storm.handler.fence.input.StopAlarmRule;
import storm.system.SysParams;
import storm.util.ConfigUtils;

/**
 * @author wza
 * 数据库操作工具类
 */
public final class Conn {
	private static final ConfigUtils configUtils = ConfigUtils.getInstance();
	private static final SysParams sysParams = SysParams.getInstance();

	static String fence_sql="SELECT fe.ID,fe.FENCE_NAME,fe.FENCE_TYPE,fe.VALID_BEGIN_TIME,fe.VALID_END_TIME,fe.FENCE_LOCATION,fe.VALID_TIME FROM SYS_FENCE_ELECTRONIC fe WHERE fe.FENCE_STATE=1";
	static String fence_rule_only_sql="SELECT tl.FENCE_ID,tl.ALARM_TYPE_CODE,tl.HEIGHEST_SPEED,tl.MINIMUM_SPEED,tl.STOP_CAR_TIME FROM SYS_FENCE_ALARM_TYPE_LK tl WHERE tl.STATE=1";
	static String fence_vid_sql="SELECT FENCE_ID,VEH_ID FROM SYS_FENCE_VEH_LK WHERE STATE=1";
	static String fence_rule_sql="SELECT at.FENCE_ALARM_NAME,at.CODE FROM SYS_FENCE_ALARM_TYPE at WHERE at.STATE=1";
	static String fence_rule_lk_sql="SELECT fe.ID,fe.FENCE_NAME,fe.FENCE_TYPE,fe.VALID_BEGIN_TIME,fe.VALID_END_TIME,fe.FENCE_LOCATION,at.FENCE_ALARM_NAME,at.CODE,tl.FENCE_ID,tl.ALARM_TYPE_CODE FROM SYS_FENCE_ELECTRONIC fe,SYS_FENCE_ALARM_TYPE at,SYS_FENCE_ALARM_TYPE_LK tl WHERE fe.FENCE_STATE=1 AND at.STATE=1 AND tl.STATE=1 AND fe.ID=tl.FENCE_ID AND at.CODE=tl.ALARM_TYPE_CODE";
	
	static String fault_rule_sql="select ID,DEPENDENCY_RULE_ID,FAULT_TYPE,STATISTICAL_TIME from SYS_SAFETY_RULES WHERE IS_DISABLE=0 and STATE='1' ";
	static String fault_alarm_lk_sql="select SYS_FAULT_ALARM_ID,SYS_DATA_CONST_ID from SYS_FAULT_ALARM_LK";
	static String fault_alarm_rule_sql="select ID,TYPE,LEVELS,NEED_CONFIRD_FLAG,L1_SEQ_NO,L2_SEQ_NO,EXPR_LEFT,EXPR_MID,R1_VAL,R2_VAL,DEPEND_ID,ALL_HOURS,START_TIME,END_TIME from SYS_DATA_CONST where IS_VALID='1'";
	static String falut_rank_sql="select ID,SYS_FAULT_ALARM_ID,LEVL,MIN_NUMBER,MAX_NUMBER,MIN_TIME,MAX_TIME from SYS_FAULT_RANK";
	static String alarm_code_sql="SELECT fc.id AS id,fc.normal_code AS CODE,'0' AS type,'0' AS LEVEL,fc.id AS codeId,fc.subordinate_parts as parts_type FROM sys_fault_code fc WHERE fc.is_delete = '0' UNION ALL SELECT fc.id AS  id,fce.exception_code AS CODE,'1' AS type,fce.response_level AS LEVEL,fce.id AS codeId,fc.subordinate_parts as parts_type FROM sys_fault_code_exception fce,sys_fault_code fc WHERE fce.fault_code_id = fc.id and fce.is_delete = '0'";

	//预警规则
	static String early_warning_sql="SELECT ID, NAME, VEH_MODEL_ID, LEVELS, DEPEND_ID, L1_SEQ_NO, EXPR_LEFT, L2_SEQ_NO, EXPR_MID, R1_VAL, R2_VAL FROM SYS_DATA_CONST WHERE TYPE = 1 AND IS_VALID = 1 AND ID is not null AND R1_VAL is not null ";
	//偏移系数自定义数据项
	static String item_coef_offset_sql="SELECT SEQ_NO,IS_ARRAY,FACTOR,OFFSET,IS_CUSTOM FROM SYS_DATA_ITEM WHERE IS_VALID = 1 AND SEQ_NO IS NOT NULL AND (OFFSET is not null OR FACTOR is not null)";
	private Connection conn;
	
	static {
		Properties sysParams = configUtils.sysParams;
		if (null != sysParams) {
			if (sysParams.containsKey("fence.sql")) {
				fence_sql = sysParams.getProperty("fence.sql");
			}
			if (sysParams.containsKey("fence.rule.only.sql")) {
				fence_rule_only_sql = sysParams.getProperty("fence.rule.only.sql");
			}
			if (sysParams.containsKey("fence.vid.sql")) {
				fence_vid_sql = sysParams.getProperty("fence.vid.sql");
			}
			if (sysParams.containsKey("fence.rule.sql")) {
				fence_rule_sql = sysParams.getProperty("fence.rule.sql");
			}
			if (sysParams.containsKey("fence.rule.lk.sql")) {
				fence_rule_lk_sql = sysParams.getProperty("fence.rule.lk.sql");
			}

            final String alarmCodeSql = Conn.sysParams.getAlarmCodeSql();
            if (null != alarmCodeSql) {
				alarm_code_sql = alarmCodeSql;
			}
			if (sysParams.containsKey("fault.rule.sql")) {
				fault_rule_sql = sysParams.getProperty("fault.rule.sql");
			}
			if (sysParams.containsKey("fault.alarm.lk.sql")) {
				fault_alarm_lk_sql = sysParams.getProperty("fault.alarm.lk.sql");
			}
			if (sysParams.containsKey("fault.alarm.rule.sql")) {
				fault_alarm_rule_sql = sysParams.getProperty("fault.alarm.rule.sql");
			}
			if (sysParams.containsKey("falut.rank.sql")) {
				falut_rank_sql = sysParams.getProperty("falut.rank.sql");
			}
			if (sysParams.containsKey("early.warning.sql")) {
				early_warning_sql = sysParams.getProperty("early.warning.sql");
			}
			if (sysParams.containsKey("item.coef.offset.sql")) {
				item_coef_offset_sql = sysParams.getProperty("item.coef.offset.sql");
			}
		}
	}
	public Connection getConn(){
		try {
			Properties p = configUtils.sysDefine;
			String driver = p.getProperty("jdbc.driver");
			String url = p.getProperty("jdbc.url");
			String username = p.getProperty("jdbc.username");
			String password = p.getProperty("jdbc.password");
			Connection conn = null;
			Class.forName(driver);
			conn = DriverManager.getConnection(url, username, password);
			return conn;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void get(Connection conn,String sql){
		Statement s = null;
		ResultSet rs = null;
		try {
			s = conn.createStatement();
			rs = s.executeQuery(sql);
			while(rs.next()){
				
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(rs,s,conn);
		}
	}
	
	public Collection<FaultRule> getFaultAndDepends(){
		try {
			Map<String, FaultRule>faultMaps = getFaults();
			List<String[]> faultLkAlarm = getFaultLkAlarm();
			Map<String,FaultAlarmRule> alarmRules = getFaultAlarmRules();
			List<String[]> rankDefs = getFaultRanks();
			if (null == faultMaps
					|| null == faultLkAlarm
					|| null == alarmRules
					|| null == rankDefs) {
				return null;
			}
			for (String[] alarmlks :  faultLkAlarm) {
				if (isNullOrEmpty(alarmlks[0]) 
						|| isNullOrEmpty(alarmlks[1])) {
					continue;
				}
				FaultRule faultRule = faultMaps.get(alarmlks[0]);
				if(null != faultRule){
					
					FaultAlarmRule alarmRule = alarmRules.get(alarmlks[1]);
					faultRule.addFaultAlarmRule(alarmRule);
					faultMaps.put(alarmlks[0], faultRule);
				}
			}
			
			for (String[] srs : rankDefs) {
				if(null != srs){
					boolean isNull = false;
					for (String string : srs) {
						if(isNullOrEmpty(string)){
							isNull = true;
							break;
						}
					}
					if(!isNull){
						RiskDef def = new RiskDef(srs[0], srs[1], Integer.valueOf(srs[2]), Integer.valueOf(srs[3]), Integer.valueOf(srs[4]), Integer.valueOf(srs[5]), Integer.valueOf(srs[6]));
						FaultRule faultRule = faultMaps.get(def.faultRuleId);
						if(null != faultRule){
							faultRule.addDef(def);
							faultMaps.put(def.faultRuleId, faultRule);
						}
					}
				}
			}
			for (Map.Entry<String, FaultRule> entry : faultMaps.entrySet()) {
				String faultId = entry.getKey();
				FaultRule rule = entry.getValue();
				rule.build();
				faultMaps.put(faultId, rule);
			}
			for (Map.Entry<String, FaultRule> entry : faultMaps.entrySet()) {
				String faultId = entry.getKey();
				FaultRule rule = entry.getValue();
				if(!isNullOrEmpty(rule.dependId)){
					FaultRule dependRule = faultMaps.get(rule.dependId);
					rule.addFaultRule(dependRule);
				}
				faultMaps.put(faultId, rule);
			}
			return faultMaps.values();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public Collection<FaultRuleCode> getFaultAlarmCodes(){
		List<Object[]> objects = getFaultRuleCodeObjects();
		Map<String, FaultRuleCode> ruleMap = new HashMap<String, FaultRuleCode>();
		if (null != objects && objects.size()>0) {
			for (Object[] objs : objects) {
				String ruleId = (String)objs[0];
				String code = (String)objs[1];
				int type = (int)objs[2];
				int level = (int)objs[3];
				String codeId = (String)objs[4];
				String itemType = (String)objs[5];
				FaultRuleCode ruleCode = ruleMap.get(ruleId);
				if (null == ruleCode) {
					ruleCode = new FaultRuleCode(ruleId,itemType);
				}
				FaultCode faultCode = new FaultCode(codeId,code, level, type);
				ruleCode.addFaultCode(faultCode);
				ruleMap.put(ruleId, ruleCode);
			}
		}
		if (ruleMap.size() > 0) {
			return ruleMap.values();
		}
		return null;
	}
	
	private Map<String, FaultRule> getFaults(){
		Map<String, FaultRule> faults=null;
		Connection conn =null;
		Statement s = null;
		ResultSet rs = null;
		try {
            if (StringUtils.isEmpty(fault_rule_sql)) {
				return null;
			}
			if (null == conn || conn.isClosed()) {
				conn = getConn();
			}
			if (null == conn) {
				return null;
			}
			faults = new HashMap<String, FaultRule>();
			s = conn.createStatement();
			rs = s.executeQuery(fault_rule_sql);
			while(rs.next()){
				FaultRule rule = new FaultRule();
				rule.id = rs.getString(1);
				rule.dependId = rs.getString(2);
				rule.type = rs.getString(3);
				rule.statisticalTime = rs.getString(4);
				faults.put(rule.id, rule);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(rs,s,conn);
		}
		return faults;
	}
	
	private List<String[]> getFaultLkAlarm(){
		List<String[]> rules=null;
		Connection conn =null;
		Statement s = null;
		ResultSet rs = null;
		try {
            if (StringUtils.isEmpty(fault_alarm_lk_sql)) {
				return null;
			}
			if (null == conn || conn.isClosed()) {
				conn = getConn();
			}
			if (null == conn) {
				return null;
			}
			rules = new LinkedList<String[]>();
			s = conn.createStatement();
			rs = s.executeQuery(fault_alarm_lk_sql);
			while(rs.next()){
				String [] strings=new String[]{rs.getString(1),rs.getString(2)};
				rules.add(strings);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(rs,s,conn);
		}
		return rules;
	}

	private List<Object[]> getFaultRuleCodeObjects() {

		final List<Object[]> rules = new LinkedList<>();

		Connection connection =null;
		Statement statement = null;
		ResultSet resultSet = null;

		try {
            if (StringUtils.isEmpty(alarm_code_sql)) {
				return rules;
			}
			if (null == connection || connection.isClosed()) {
				connection = getConn();
			}
			if (null == connection) {
				return rules;
			}

            statement = connection.createStatement();
			resultSet = statement.executeQuery(alarm_code_sql);
			while(resultSet.next()) {
				Object [] rule = new Object[] {
				    // 故障码种类 ID
				    resultSet.getString(1),
                    // 异常码/恢复码
                    resultSet.getString(2),
                    // 码类型, 0-恢复码, 1-异常码
                    resultSet.getInt(3),
                    // 故障级别
                    resultSet.getInt(4),
                    // 故障码/异常码 ID
                    resultSet.getString(5),
                    // 车型
                    resultSet.getString(6)
				};
				rules.add(rule);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(resultSet,statement,connection);
		}
		return rules;
	}
	
	private Map<String,FaultAlarmRule> getFaultAlarmRules(){
		Map<String,FaultAlarmRule> rules=null;
		Connection conn =null;
		Statement s = null;
		ResultSet rs = null;
		try {
            if (StringUtils.isEmpty(fault_alarm_rule_sql)) {
				return null;
			}
			if (null == conn || conn.isClosed()) {
				conn = getConn();
			}
			if (null == conn) {
				return null;
			}
			rules = new HashMap<String,FaultAlarmRule>();
			s = conn.createStatement();
			rs = s.executeQuery(fault_alarm_rule_sql);
			while(rs.next()){
				// ID,TYPE,LEVELS,NEED_CONFIRD_FLAG,L1_SEQ_NO,L2_SEQ_NO,EXPR_LEFT,EXPR_MID,R1_VAL,R2_VAL,DEPEND_ID,ALL_HOURS,START_TIME,END_TIME
				String ruleId = rs.getString(1);
				if (! isNullOrEmpty(ruleId)) {
					
					FaultAlarmRule rule = new FaultAlarmRule();
					rule.id = ruleId;
					rule.type = rs.getInt(2);
					rule.risk = rs.getInt(3);
					rule.leftField1 = rs.getString(5);
					rule.leftField2 = rs.getString(6);
					rule.leftOper1 = rs.getString(7);
					rule.leftOper2 = rs.getString(8);
					rule.rightVal1 = rs.getFloat(9);
					rule.rightVal1 = rs.getFloat(10);
					rule.dependId = rs.getString(11);
					rule.timeString = rs.getString(12)+"|"+rs.getString(13)+"|"+rs.getString(14);
					rule.bulid();
					rules.put(ruleId, rule);
				}
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(rs,s,conn);
		}
		return rules;
	}
	
	private List<String[]> getFaultRanks(){
		List<String[]> rules=null;
		Connection conn =null;
		Statement s = null;
		ResultSet rs = null;
		try {
            if (StringUtils.isEmpty(falut_rank_sql)) {
				return null;
			}
			if (null == conn || conn.isClosed()) {
				conn = getConn();
			}
			if (null == conn) {
				return null;
			}
			rules = new LinkedList<String[]>();
			s = conn.createStatement();
			rs = s.executeQuery(falut_rank_sql);
			while(rs.next()){
				String [] strings=new String[]{rs.getString(1),rs.getString(2),
						""+rs.getInt(3),rs.getString(4),rs.getString(5),
						rs.getString(6),rs.getString(7)};
				rules.add(strings);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(rs,s,conn);
		}
		return rules;
	}
	
	public List<Object []> getAllWarns(){
		List<Object[]> rules=null;
		Connection conn =null;
		Statement s = null;
		ResultSet rs = null;
		try {
            if (StringUtils.isEmpty(early_warning_sql)) {
				return null;
			}
			if (null == conn || conn.isClosed()) {
				conn = getConn();
			}
			if (null == conn) {
				return null;
			}
			rules = new LinkedList<Object[]>();
			s = conn.createStatement();
			rs = s.executeQuery(early_warning_sql);
			//ID, NAME, VEH_MODEL_ID, LEVELS, DEPEND_ID, L1_SEQ_NO, EXPR_LEFT, L2_SEQ_NO, EXPR_MID, R1_VAL, R2_VAL
			while(rs.next()){
				Object [] objects=new Object[]{rs.getString(1),rs.getString(2),
						rs.getString(3),rs.getInt(4),rs.getString(5),
						rs.getString(6),rs.getString(7),rs.getString(8),
						rs.getString(9),rs.getFloat(10),rs.getFloat(11)};
				rules.add(objects);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(rs,s,conn);
		}
		return rules;
	}
	
	public List<Object []> getAllCoefOffset(){
		List<Object[]> rules=null;
		Connection conn =null;
		Statement s = null;
		ResultSet rs = null;
		try {
            if (StringUtils.isEmpty(item_coef_offset_sql)) {
				return null;
			}
			if (null == conn || conn.isClosed()) {
				conn = getConn();
			}
			if (null == conn) {
				return null;
			}
			rules = new LinkedList<Object[]>();
			s = conn.createStatement();
			rs = s.executeQuery(item_coef_offset_sql);
			//SEQ_NO,IS_ARRAY,FACTOR,OFFSET,IS_CUSTOM
			while(rs.next()){
				Object [] objects=new Object[]{rs.getString(1),rs.getInt(2),
						rs.getDouble(3),rs.getDouble(4),rs.getInt(5)};
				rules.add(objects);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(rs,s,conn);
		}
		return rules;
	}
	
	/**
	 * 返回 数据结构 vid -list EleFence
	 * @return
	 */
	public Map<String, List<EleFence>> vidFences(){
		
		List<String[]> vidFenceMap = getVidFenceMap();
		if(null == vidFenceMap || vidFenceMap.size() == 0) {
			return null;
		}
		Map<String, List<EleFence>> vidfences = new HashMap<String, List<EleFence>>();
		try {
			Map<String, EleFence> fenceMap= fencesWithId();
//		Map<String, List<String>> vidfenceIds = new HashMap<String, List<String>>();
			for (String[] strings : vidFenceMap) {
				if (null != strings && strings.length == 2) {
					if(null != strings[0] && !"".equals(strings[0].trim())
							&& null != strings[1] && !"".equals(strings[1].trim())){
						String fenceId = strings[0].trim();
						String vid = strings[1].trim();
//					List<String> list = vidfenceIds.get(vid);
//					if(null == list)
//						list = new LinkedList<String>();
//					if (!list.contains(fenceId)) 
//						list.add(fenceId);
						
						List<EleFence> fences = vidfences.get(vid);
						EleFence fence = fenceMap.get(fenceId);
						if(null == fences) {
							fences = new LinkedList<EleFence>();
						}
						if(null != fence && ! fences.contains(fence)) {
							fences.add(fence);
						}
						vidfences.put(vid, fences);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return vidfences;
	}
	
	public Map<String, EleFence> fencesWithId(){
		try {
			List<EleFence> fences = getFences();
			List<Map<String, String>> rules = getRulesMap();
			Map<String, EleFence> fenceMap = null;
			if (null == rules) {
				return fenceMap;
			}
			Map<String,List<Rule>> ruleMap= groupRulesById(rules);
			fenceMap = new HashMap<String, EleFence>();
			if (null != fences && fences.size()>0) {
				for (EleFence eleFence : fences) {
					String fenceId = eleFence.id;
					List<Rule> fenceRules = ruleMap.get(fenceId);
					if (null != fenceRules && fenceRules.size()>0) {
						eleFence.rules=fenceRules;
					}
					fenceMap.put(fenceId, eleFence);
				}
			}
			return fenceMap;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public List<EleFence> getAllFencesLinkRules(){
		try {
			List<EleFence> fences = getFences();
			List<Map<String, String>> rules = getRulesMap();
			if (null == rules) {
				return fences;
			}
			Map<String,List<Rule>> ruleMap= groupRulesById(rules);
			if (null != fences && fences.size()>0) {
				for (EleFence eleFence : fences) {
					String fenceId = eleFence.id;
					List<Rule> fenceRules = ruleMap.get(fenceId);
					if (null != fenceRules && fenceRules.size()>0) {
						eleFence.rules=fenceRules;
					}
				}
			}
			return fences;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	private List<EleFence> getFences(){
		List<EleFence> fences=null;
		Connection conn =null;
		Statement s = null;
		ResultSet rs = null;
		try {
            if (StringUtils.isEmpty(fence_sql)) {
				return null;
			}
			if (null == conn || conn.isClosed()) {
				conn = getConn();
			}
			if (null == conn) {
				return null;
			}
			fences = new LinkedList<EleFence>();
			s = conn.createStatement();
			rs = s.executeQuery(fence_sql);
			while(rs.next()){
				EleFence fence = new EleFence();
				fence.id = rs.getString(1);
				fence.name = rs.getString(2);
				fence.type = rs.getString(3);
				fence.timesegs = rs.getString(4)+"|"+rs.getString(5);
				fence.pointRange = rs.getString(6);
				fence.periodValidtime = rs.getString(7);
				fence.status="1";
				fence.build();
				fences.add(fence);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(rs,s,conn);
		}
		return fences;
	}
	
	private List<String[]> getVidFenceMap(){
		List<String[]> rules=null;
		Connection conn =null;
		Statement s = null;
		ResultSet rs = null;
		try {
            if (StringUtils.isEmpty(fence_vid_sql)) {
				return null;
			}
			if (null == conn || conn.isClosed()) {
				conn = getConn();
			}
			if (null == conn) {
				return null;
			}
			rules = new LinkedList<String[]>();
			s = conn.createStatement();
			rs = s.executeQuery(fence_vid_sql);
			while(rs.next()){
				String [] strings=new String[]{rs.getString(1),rs.getString(2)};
				rules.add(strings);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(rs,s,conn);
		}
		return rules;
	}
	
	private List<Map<String, String>> getRulesMap(){
		List<Map<String, String>> rules=null;
		Connection conn =null;
		Statement s = null;
		ResultSet rs = null;
		try {
            if (StringUtils.isEmpty(fence_rule_only_sql)) {
				return null;
			}
			if (null == conn || conn.isClosed()) {
				conn = getConn();
			}
			if (null == conn) {
				return null;
			}
			rules = new LinkedList<Map<String, String>>();
			s = conn.createStatement();
			rs = s.executeQuery(fence_rule_only_sql);
			while(rs.next()){
				Map<String, String> map = new TreeMap<String, String>();
				map.put("fenceId", rs.getString(1));
				map.put("alarmTypeCode", rs.getString(2));
				map.put("highSpeed", rs.getString(3));
				map.put("lowSpeed", rs.getString(4));
				map.put("stopTime", rs.getString(5));
				rules.add(map);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(rs,s,conn);
		}
		return rules;
	}
	
	private Map<String,List<Rule>> groupRulesById(List<Map<String, String>> rules){
		if (null == rules) {
			return null;
		}
		
		try {
			Map<String,List<Rule>> maps= new HashMap<String,List<Rule>>();
			for (Map<String, String> map : rules) {
				if (null != map && map.size()>0) {
					String fenceId = map.get("fenceId");
					String alarmTypeCode = map.get("alarmTypeCode");
					List<Rule> list = maps.get(fenceId);
					if (null == list) {
						list = new LinkedList<Rule>();
					}
					if("0001".equals(alarmTypeCode)){
						SpeedAlarmRule rule = new SpeedAlarmRule();
						rule.setCode(alarmTypeCode);
						rule.speedType = AlarmRule.GT;
						rule.speeds=new double[]{10*todouble(map.get("lowSpeed")), 10*todouble(map.get("highSpeed"))};
						list.add(rule);
					} else if("0002".equals(alarmTypeCode)){
						SpeedAlarmRule rule = new SpeedAlarmRule();
						rule.setCode(alarmTypeCode);
						rule.speedType = AlarmRule.LT;
						rule.speeds=new double[]{10*todouble(map.get("lowSpeed")), 10*todouble(map.get("highSpeed"))};
						list.add(rule);
					} else if("0001,0002".equals(alarmTypeCode)){
						SpeedAlarmRule rule = new SpeedAlarmRule();
						rule.speedType = AlarmRule.GLT;
						rule.setCode(alarmTypeCode);
						rule.speeds=new double[]{10*todouble(map.get("lowSpeed")), 10*todouble(map.get("highSpeed"))};
						list.add(rule);
					} else if("0009".equals(alarmTypeCode)){
						StopAlarmRule rule = new StopAlarmRule();
						rule.stopType = AlarmRule.IN;
						int stopTime = toInt(map.get("stopTime"));
						if (stopTime>0) {
							rule.stopTime = stopTime*60;
						}
						rule.setCode(alarmTypeCode);
						list.add(rule);
					} else if("0010".equals(alarmTypeCode)){
						StopAlarmRule rule = new StopAlarmRule();
						rule.stopType = AlarmRule.OUT;
						int stopTime = toInt(map.get("stopTime"));
						if (stopTime>0) {
							rule.stopTime = stopTime*60;
						}
						rule.setCode(alarmTypeCode);
						list.add(rule);
					} else if("0009,0010".equals(alarmTypeCode)){
						StopAlarmRule rule = new StopAlarmRule();
						rule.stopType = AlarmRule.INOUT;
						int stopTime = toInt(map.get("stopTime"));
						if (stopTime>0) {
							rule.stopTime = stopTime*60;
						}
						rule.setCode(alarmTypeCode);
						list.add(rule);
					}
					maps.put(fenceId, list);
				}
			}
			return maps;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	private int toInt(String str){
		String numst = stringDoubleNumber(str);
		int poidx = numst.indexOf(".");
		if(0 < poidx) {
			numst = numst.substring(0, poidx);
		}
		return Integer.valueOf(numst);
	}
	private double todouble(String str){
		String numst = stringDoubleNumber(str);
		return Double.valueOf(numst);
	}
	private String stringDoubleNumber(String str) {
        if (!StringUtils.isEmpty(str) && str.matches("[0-9]*.[0-9]{0,10}")) {
            return str;
        }
        return "-1";
    }
	private Map<String,List<Map<String,String>>> groupById(List<Map<String, String>> rules){
		if (null == rules) {
			return null;
		}
		
		Map<String,List<Map<String,String>>> maps= new HashMap<String,List<Map<String, String>>>();
		for (Map<String, String> map : rules) {
			if (null != map && map.size()>0) {
				String fenceId = map.get("fenceId");
				List<Map<String,String>> list = maps.get(fenceId);
				if (null == list) {
					list = new LinkedList<Map<String,String>>();
				}
				list.add(map);
				maps.put(fenceId, list);
			}
		}
		return maps;
	}
	
	public List<Map<String, Object>> get(String sql,String[]filedName){
		List<Map<String, Object>>list=null;
		Connection conn =null;
		Statement s = null;
		ResultSet rs = null;
		try {
			if (null == conn || conn.isClosed()) {
				conn = getConn();
			}
			if (null == conn) {
				return null;
			}
			if (null == filedName || filedName.length<1) {
				return list;
			}
			list = new LinkedList<Map<String, Object>>();
			s = conn.createStatement();
			rs = s.executeQuery(sql);
			while(rs.next()){
				Map<String,Object> map = new TreeMap<String,Object>();
				for (int i = 0; i < filedName.length; i++) {
					if (null != filedName[i] && !"".equals(filedName[i].trim())) {
						map.put(filedName[i], rs.getObject(i+1));
					}
				}
				list.add(map);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(rs,s,conn);
		}
		return list;
	}
	
	boolean isNullOrEmpty(String string){
		if(null == string || "".equals(string)) {
			return true;
		}
		return "".equals(string.trim());
	}

    /**
     * 释放系统资源
     * @param resources
     */
	private void close(AutoCloseable ... resources){
		if (null != resources && resources.length > 0) {
			for (AutoCloseable resource : resources) {
				if (null != resource) {
					try {
						resource.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public static void main(String[] args) {
		Conn conn = new Conn();
		
		
		Collection<FaultRule> faultRules = conn.getFaultAndDepends();
		
		for (FaultRule faultRule : faultRules) {
			System.out.print(faultRule.toString());
			List<FaultAlarmRule>alarmRules = faultRule.dependAlarmRules;
			if (null != alarmRules) {
				for (FaultAlarmRule faultAlarmRule : alarmRules) {
					System.out.print(faultAlarmRule);
				}
			}
			System.out.println();
		}
		
	}
}
