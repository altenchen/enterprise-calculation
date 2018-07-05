package storm.util.dbconn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dto.ExceptionSingleBit;
import storm.dto.FaultCodeByte;
import storm.dto.FaultCodeByteRule;
import storm.dto.FaultTypeSingleBit;
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
	private static final Logger logger = LoggerFactory.getLogger(Conn.class);

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


    /**
     * 故障码按字节解析规则
     * SELECT
     *   faultCode.id fault_id,
     *   faultCode.fault_type,
     *   faultCode.analyze_type,
     *   GROUP_CONCAT(model.id) model_num,
     *   '0' AS exception_type,
     *   faultCode.id AS exception_id,
     *   faultCode.normal_code exception_code,
     *   '0' AS response_level
     * FROM sys_fault_code faultCode
     *   LEFT JOIN sys_fault_code_model code_model ON faultCode.id=code_model.fault_code_id
     *   LEFT JOIN sys_veh_model model ON model.id=code_model.veh_model_id
     * WHERE faultCode.is_delete = '0'
     *   AND faultCode.analyze_type = '1'
     * GROUP BY faultCode.id
     * UNION ALL SELECT
     *   faultCode.id fault_id,
     *   faultCode.fault_type,
     *   faultCode.analyze_type,
     *   GROUP_CONCAT(model.id) model_num,
     *   '1' AS exception_type,
     *   excep.id AS exception_id,
     *   excep.exception_code exception_code,
     *   excep.response_level AS response_level
     * FROM sys_fault_code_exception excep
     *   LEFT JOIN sys_fault_code faultCode ON excep.fault_code_id=faultCode.id
     *   LEFT JOIN sys_fault_code_model code_model ON faultCode.id=code_model.fault_code_id
     *   LEFT JOIN sys_veh_model model ON model.id=code_model.veh_model_id
     * WHERE excep.is_delete = '0'
     *   AND faultCode.is_delete = '0'
     *   AND faultCode.analyze_type = '1'
     */
	private static String alarm_code_sql="SELECT faultCode.id fault_id,faultCode.fault_type,faultCode.analyze_type,GROUP_CONCAT(model.id) model_num,'0' AS exception_type,faultCode.id AS exception_id,faultCode.normal_code exception_code,'0' AS response_level FROM sys_fault_code faultCode LEFT JOIN sys_fault_code_model code_model ON faultCode.id=code_model.fault_code_id LEFT JOIN sys_veh_model model ON model.id=code_model.veh_model_id WHERE faultCode.is_delete='0' AND faultCode.analyze_type='1' GROUP BY faultCode.id UNION ALL SELECT faultCode.id fault_id,faultCode.fault_type,faultCode.analyze_type,GROUP_CONCAT(model.id) model_num,'1' AS exception_type,excep.id AS exception_id,excep.exception_code exception_code,excep.response_level AS response_level FROM sys_fault_code_exception excep LEFT JOIN sys_fault_code faultCode ON excep.fault_code_id=faultCode.id LEFT JOIN sys_fault_code_model code_model ON faultCode.id=code_model.fault_code_id LEFT JOIN sys_veh_model model ON model.id=code_model.veh_model_id WHERE excep.is_delete='0' AND faultCode.is_delete='0' AND faultCode.analyze_type='1'";

    /**
     * 故障码按位解析规则
     * SELECT
     *   faultCode.id fault_id,
     *   faultCode.fault_type,
     *   faultCode.analyze_type,
     *   faultCode.param_length,
     *   GROUP_CONCAT(model.id) model_num,
     *   excep.start_point,
     *   excep.id exception_id,
     *   excep.exception_code,
     *   faultCode.threshold time_threshold,
     *   excep.response_level
     * FROM sys_fault_code_exception excep
     *   LEFT JOIN sys_fault_code faultCode ON excep.fault_code_id=faultCode.id
     *   LEFT JOIN sys_fault_code_model code_model ON faultCode.id=code_model.fault_code_id
     *   LEFT JOIN sys_veh_model model ON model.id=code_model.veh_model_id
     * WHERE excep.is_delete = '0'
     *   AND faultCode.is_delete = '0'
     *   AND faultCode.analyze_type = '2'
     * GROUP BY excep.id
     */
	private static String alarm_code_bit_sql = "SELECT faultCode.id fault_id,faultCode.fault_type,faultCode.analyze_type,faultCode.param_length,GROUP_CONCAT(model.id) model_num,excep.start_point,excep.id exception_id,excep.exception_code,faultCode.threshold time_threshold,excep.response_level FROM sys_fault_code_exception excep LEFT JOIN sys_fault_code faultCode ON excep.fault_code_id=faultCode.id LEFT JOIN sys_fault_code_model code_model ON faultCode.id=code_model.fault_code_id LEFT JOIN sys_veh_model model ON model.id=code_model.veh_model_id WHERE excep.is_delete='0' AND faultCode.is_delete='0' AND faultCode.analyze_type='2' GROUP BY excep.id";

    /**
     * 车辆车型表
     * SELECT
     *   veh.uuid vid,
     *   model.id modelId
     * FROM sys_vehicle veh
     *   LEFT JOIN sys_veh_model model ON veh.veh_model_id=model.id
     * WHERE model.id IS NOT NULL
     */
	private static String veh_model_sql = "SELECT veh.uuid vid,model.id mid FROM sys_vehicle veh LEFT JOIN sys_veh_model model ON veh.veh_model_id=model.id WHERE model.id IS NOT NULL";

    /**
     * 预警规则
     */
	static String early_warning_sql="SELECT ID, NAME, VEH_MODEL_ID, LEVELS, DEPEND_ID, L1_SEQ_NO, EXPR_LEFT, L2_SEQ_NO, EXPR_MID, R1_VAL, R2_VAL FROM SYS_DATA_CONST WHERE TYPE = 1 AND IS_VALID = 1 AND ID is not null AND R1_VAL is not null ";
    /**
     * 偏移系数自定义数据项
     */
	static String item_coef_offset_sql="SELECT SEQ_NO,IS_ARRAY,FACTOR,OFFSET,IS_CUSTOM FROM SYS_DATA_ITEM WHERE IS_VALID = 1 AND SEQ_NO IS NOT NULL AND (OFFSET is not null OR FACTOR is not null)";

	private Connection conn;
	
	static {
		Properties sysParams = configUtils.sysParams;
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

        alarm_code_bit_sql = sysParams.getProperty(
            SysParams.ALARM_CODE_BIT_SQL,
            alarm_code_bit_sql);
        veh_model_sql = sysParams.getProperty(
            SysParams.VEH_MODEL_SQL,
            veh_model_sql);
    }
	public Connection getConn(){
		try {
			Properties p = configUtils.sysDefine;
			String driver = p.getProperty("jdbc.driver");
			String url = p.getProperty("jdbc.url");
			String username = p.getProperty("jdbc.username");
			String password = p.getProperty("jdbc.password");
			Class.forName(driver);
            return DriverManager.getConnection(url, username, password);
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
			Map<String, storm.dto.fault.FaultAlarmRule> alarmRules = getFaultAlarmRules();
			List<String[]> rankDefs = getFaultRanks();
			if (null == faultMaps
					|| null == faultLkAlarm
					|| null == alarmRules
					|| null == rankDefs) {
				return null;
			}
			for (String[] alarmlks :  faultLkAlarm) {
                if (StringUtils.isBlank(alarmlks[0])
						|| StringUtils.isBlank(alarmlks[1])) {
					continue;
				}
				FaultRule faultRule = faultMaps.get(alarmlks[0]);
				if(null != faultRule){
					
					storm.dto.fault.FaultAlarmRule alarmRule = alarmRules.get(alarmlks[1]);
					faultRule.addFaultAlarmRule(alarmRule);
					faultMaps.put(alarmlks[0], faultRule);
				}
			}
			
			for (String[] srs : rankDefs) {
				if(null != srs){
					boolean isNull = false;
					for (String string : srs) {
                        if(StringUtils.isBlank(string)){
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
                if(!StringUtils.isBlank(rule.dependId)){
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
    /**
     * @return 故障码报警规则
     */
    @NotNull
	public Collection<FaultCodeByteRule> getFaultAlarmCodes(){
        final Map<String, FaultCodeByteRule> result = new HashMap<>();

		final List<String[]> rules = getFaultRuleCodeObjects();

        final byte fault_id = 0;
        final byte fault_type = 1;
        final byte analyze_type = 2;
        final byte model_num = 3;
        final byte exception_type = 4;
        final byte exception_id = 5;
        final byte exception_code = 6;
        final byte response_level = 7;

        for (String[] objs : rules) {

            final String faultId = objs[fault_id];
            if(StringUtils.isBlank(faultId)) {
                continue;
            }

            final String faultType = objs[fault_type];
            if(StringUtils.isBlank(faultType)) {
                logger.trace("故障码[{}]: 空白的故障码类型.", faultType);
                continue;
            }

            final String analyzeType = objs[analyze_type];
            if(StringUtils.isBlank(analyzeType)) {
                logger.trace("故障码[{}]: 无效的解析方式[{}].", faultType, analyzeType);
                continue;
            }
            // 解析方式 1-按字节, 2-按位; 这里只处理按字节解析的情况
            if(!"1".equals(analyzeType)) {
                continue;
            }

            // 适用车型, 车型Id用英文逗号衔接, 如果为空, 则适用于所有车型.
            final String[] modelNum = Arrays.stream(StringUtils
                .defaultIfEmpty(
                    objs[model_num],
                    "")
                .split("\\s*,\\s*"))
                .filter(s -> StringUtils.isNotBlank(s))
                .distinct()
                .toArray(String[]::new);
            // 原逻辑没处理车型, 暂时不动

            // 异常类型, 1-正常码, 2-异常码
            final int exceptionType;
            try {
                exceptionType = Integer.parseInt(objs[exception_type]);
            } catch (NumberFormatException ex) {
                ex.printStackTrace();
                logger.error("故障码[{}]: 无效的异常类型[{}].", faultType, objs[exception_type]);
                continue;
            }
            if(exceptionType != 0 && exceptionType != 1) {
                logger.error("故障码[{}]: 错误的异常类型[{}].", faultType, exceptionType);
                continue;
            }

            String exceptionId = objs[exception_id];
            if(StringUtils.isBlank(exceptionId)) {
                logger.error("故障码[{}]: 空白的异常码Id.", faultType);
                continue;
            }

            String exceptionCode = objs[exception_code];
            if(StringUtils.isBlank(exceptionCode)) {
                logger.error("故障码[{}]异常码[{}]: 空白的异常码值.", faultType, exceptionId);
                continue;
            }

            final int responseLevel;
            try {
                responseLevel = Integer.parseInt(objs[response_level]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                logger.error("故障码[{}]异常码[{}]: 错误的告警等级[{}].", faultType, exceptionId, objs[response_level]);
                continue;
            }

            FaultCodeByteRule ruleCode = result.get(faultId);
            if (null == ruleCode) {
                ruleCode = new FaultCodeByteRule(faultId, faultType);
            }

            FaultCodeByte faultCode = new FaultCodeByte(exceptionId,exceptionCode, responseLevel, exceptionType);
            ruleCode.addFaultCode(faultCode);

            result.put(faultId, ruleCode);
        }

		return result.values();
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


    /**
     * 获取车辆车型关系
     * @return Key-车辆Id, Value-车型Id
     */
    public Map<String, String> getVehicleModel() {

        final Map<String, String> vmd = new TreeMap<>();

        if (StringUtils.isBlank(veh_model_sql)) {
            return vmd;
        }

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            connection = getConn();
            if (null == connection) {
                logger.warn("创建数据库连接失败");
                return vmd;
            }

            statement = connection.createStatement();
            resultSet = statement.executeQuery(veh_model_sql);

            while(resultSet.next()) {

                // 车辆Id
                final String vid = resultSet.getString(1);
                if(StringUtils.isBlank(vid)) {
                    continue;
                }

                final String mid = resultSet.getString(2);
                if(StringUtils.isBlank(mid)) {
                    continue;
                }

                vmd.put(vid, mid);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(resultSet, statement, connection);
        }

        return vmd;
    }

    /**
     * @return 从数据库拉取数据构建完整的按位解析故障码规则
     */
    @SuppressWarnings("Duplicates")
    @NotNull
    public Map<String, FaultTypeSingleBit> getFaultSingleBitRules() {
        logger.info("开始更新按位解析故障码规则");

        // fault_type, fault
        final Map<String, FaultTypeSingleBit> faultTypes = new TreeMap<>();

        if (StringUtils.isBlank(alarm_code_bit_sql)) {
            return faultTypes;
        }

//        if (StringUtils.isBlank(alarm_code_sql)) {
//            return faultTypes;
//        }

        Connection connection = null;
        Statement statement = null;
        ResultSet analyzeBitResult = null;
        //ResultSet analyzeValueResult = null;

        try {
            connection = getConn();
            if (null == connection) {
                logger.warn("创建数据库连接失败");
                return faultTypes;
            }

            statement = connection.createStatement();
            analyzeBitResult = statement.executeQuery(alarm_code_bit_sql);
            //analyzeValueResult = statement.executeQuery(alarm_code_sql);

            // exception_id, exception
            final Map<String, ExceptionSingleBit> faultExceptionsCache = new TreeMap<>();

            while(analyzeBitResult.next()) {

                // 故障码Id
                final String fault_id = analyzeBitResult.getString(1);
                if(StringUtils.isBlank(fault_id)) {
                    continue;
                }

                // 故障码类型(内部协议标号)
                final String fault_type = analyzeBitResult.getString(2);
                if (StringUtils.isBlank(fault_id)) {
                    logger.trace("故障码[{}]: 空白的故障码类型.", fault_id);
                    continue;
                }

                // 解析方式 1-按字节, 2-按位
                final String analyze_type = analyzeBitResult.getString(3);
                if (StringUtils.isEmpty(analyze_type) || !StringUtils.isNumeric(analyze_type)) {
                    logger.trace("故障码[{}]: 无效的解析方式[{}].", fault_id, analyze_type);
                    continue;
                }

                final boolean isAnalyzeByBit = "2".equals(analyze_type);

				// 目前只处理按1位解析规则, 其它的走老规则
				if(isAnalyzeByBit) {

                    // 位长, 目前固定为1
                    final String param_length = analyzeBitResult.getString(4);
                    if (StringUtils.isEmpty(param_length) || !StringUtils.isNumeric(param_length)) {
                        logger.trace("故障码[{}]: 按位解析方式下, 空白的位长类型.", fault_id);
                        continue;
                    }

                    // 目前只处理按1位解析规则, 其它的走老规则
                    if(param_length != "1") {
                        continue;
                    }

                    // 适用车型, 车型Id用英文逗号衔接, 如果为空, 则适用于所有车型.
                    final String[] model_num = Arrays.stream(StringUtils
                        .defaultIfEmpty(
                            analyzeBitResult.getString(5),
                            "")
                        .split("\\s*,\\s*"))
                        .filter(s -> StringUtils.isNotBlank(s))
                        .distinct()
                        .toArray(String[]::new);

                    // 起始位偏移量
                    final String start_point = analyzeBitResult.getString(6);
                    if (StringUtils.isEmpty(start_point) || !StringUtils.isNumeric(start_point)) {
                        logger.trace("故障码[{}]: 按位解析方式下, 空白的起始位偏移量.", fault_id);
                        continue;
                    }

                    // 异常码Id
                    final String exception_id = analyzeBitResult.getString(7);
                    if (StringUtils.isBlank(exception_id)) {
                        logger.trace("故障码[{}]: 按位解析方式下, 空白的异常码Id.", fault_id);
                        continue;
                    }

                    final Short faultOffset;
                    try {
                        faultOffset = Short.decode(start_point);
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                        logger.warn(
                            "故障码[{}]异常码[{}]: 起始位偏移量格式错误: {}",
                            fault_id,
                            exception_id,
                            e.getLocalizedMessage());
                        continue;
                    }

                    // 异常码码值, 应为目前固定位长为1, 所以码值也只有1了
                    final String exception_code = analyzeBitResult.getString(8);
                    if (StringUtils.isEmpty(exception_code) || !StringUtils.isNumeric(exception_code)) {
                        logger.trace("故障码[{}]: 按位解析方式下, 空白的异常码码值.", fault_id);
                        continue;
                    }

                    // 时间阈值
                    final String time_threshold = analyzeBitResult.getString(9);
                    if (StringUtils.isEmpty(time_threshold) || !StringUtils.isNumeric(time_threshold)) {
                        logger.trace("故障码[{}]: 按位解析方式下, 空白的时间阈值.", fault_id);
                        continue;
                    }

                    final int lazy;
                    try {
                        lazy = Integer.decode(time_threshold);
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                        logger.warn(
                            "故障码[{}]异常码[{}]: 时间阈值格式错误: {}",
                            fault_id,
                            exception_id,
                            e.getLocalizedMessage());
                        continue;
                    }

                    // 告警等级
                    final String response_level = analyzeBitResult.getString(10);
                    if (StringUtils.isEmpty(response_level) || !StringUtils.isNumeric(response_level)) {
                        logger.trace("故障码[{}]: 按位解析方式下, 空白的告警等级.", fault_id);
                        continue;
                    }

                    final byte level;
                    try {
                        level = Byte.decode(time_threshold);
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                        logger.warn(
                            "故障码[{}]异常码[{}]: 告警等级格式错误: {}",
                            fault_id,
                            exception_id,
                            e.getLocalizedMessage());
                        continue;
                    }

                    // Key-故障类型
                    final FaultTypeSingleBit faultTypeSingleBit;
                    if(faultTypes.containsKey(fault_type)) {
                        faultTypeSingleBit = faultTypes.get(fault_type);
                    } else {
                        faultTypeSingleBit = new FaultTypeSingleBit(fault_id, fault_type, analyze_type);
                        faultTypes.put(faultTypeSingleBit.faultType, faultTypeSingleBit);
                    }

                    if (faultExceptionsCache.containsKey(exception_id)) {
                        logger.warn("重复的异常码[{}].", exception_id);
                        continue;
                    }

                    final ExceptionSingleBit exception = new ExceptionSingleBit(exception_id, faultOffset, lazy, level);
                    faultExceptionsCache.put(exception.exceptionId, exception);

                    // 将异常关联到适用车型, 空字符串表示默认车型
                    final String[] vehicleModels = ArrayUtils.isEmpty(model_num) ? new String[] {""} : model_num;
                    for (String vehicleModel : vehicleModels) {
                        final Map<String, ExceptionSingleBit> exceptions =
                            faultTypeSingleBit.ensureVehExceptions(vehicleModel);
                        if(exceptions.containsKey(exception.exceptionId)) {
                            logger.warn(
                                "故障码[{}]车型[{}]重复的异常码[{}].",
                                faultTypeSingleBit.faultId,
                                vehicleModel,
                                exception.exceptionId);
                            continue;
                        }
                        exceptions.put(exception.exceptionId, exception);
                    }
                }
            }
            logger.info("更新获取到[{}]条有效按位解析故障码规则", faultExceptionsCache.size());


//            while(analyzeValueResult.next()) {
//
//                // 故障码种类 ID
//                final String fault_id = analyzeValueResult.getString(1);
//                // 异常码/恢复码 码值
//                final String exception_code = analyzeValueResult.getString(2);
//                // 码类型, 0-恢复码, 1-异常码
//                final int type = analyzeValueResult.getInt(3);
//                // 故障级别
//                final int level = analyzeValueResult.getInt(4);
//                // 故障码/异常码 ID
//                final String exception_id = analyzeValueResult.getString(5);
//                // 所需零部件(废除)
//                final String parts_type = analyzeValueResult.getString(6);
//
//                FaultCodeByteRule ruleCode = new FaultCodeByteRule(fault_id, parts_type);
//
//                FaultCodeByte faultCode = new FaultCodeByte(exception_id,exception_code, level, type);
//                ruleCode.addFaultCode(faultCode);
//            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            //close(analyzeValueResult, analyzeBitResult, statement, connection);
            close(analyzeBitResult, statement, connection);
        }

        return faultTypes;
    }

    /**
     * @return 获取故障码规则
     */
    @NotNull
	private List<String[]> getFaultRuleCodeObjects() {

		final List<String[]> rules = new LinkedList<>();

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
                String [] rule = new String[] {
				    // fault_id
				    resultSet.getString(1),
                    // fault_type
                    resultSet.getString(2),
                    // analyze_type
                    resultSet.getString(3),
                    // model_num
                    resultSet.getString(4),
                    // exception_type
                    resultSet.getString(5),
                    // exception_id
                    resultSet.getString(6),
                    // exception_code
                    resultSet.getString(7),
                    // response_level
                    resultSet.getString(8),
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
	
	private Map<String, storm.dto.fault.FaultAlarmRule> getFaultAlarmRules(){
		Map<String, storm.dto.fault.FaultAlarmRule> rules=null;
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
			rules = new HashMap<String, storm.dto.fault.FaultAlarmRule>();
			s = conn.createStatement();
			rs = s.executeQuery(fault_alarm_rule_sql);
			while(rs.next()){
				// ID,TYPE,LEVELS,NEED_CONFIRD_FLAG,L1_SEQ_NO,L2_SEQ_NO,EXPR_LEFT,EXPR_MID,R1_VAL,R2_VAL,DEPEND_ID,ALL_HOURS,START_TIME,END_TIME
				String ruleId = rs.getString(1);
                if (!StringUtils.isBlank(ruleId)) {
					
					storm.dto.fault.FaultAlarmRule rule = new storm.dto.fault.FaultAlarmRule();
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

        final Map<String, String> vmd = conn.getVehicleModel();
        System.out.println("vmd.size=" + vmd.size());
        vmd.forEach((k, v) -> System.out.println(k + " -> " + v));

//        final Map<String, FaultTypeSingleBit> faultRuleCodeBitObjects = conn.getFaultSingleBitRules();
//        for (FaultTypeSingleBit faultRuleCodeBitObject : faultRuleCodeBitObjects.values()) {
//            LOG.debug("------------");
//            final String json = JSON.toJSONString(faultRuleCodeBitObject, true);
//            LOG.debug(json);
//        }

//        Collection<FaultRule> faultRules = conn.getFaultAndDepends();
//
//		for (FaultRule faultRule : faultRules) {
//			System.out.print(faultRule.toString());
//			List<storm.dto.fault.FaultAlarmRule>alarmRules = faultRule.dependAlarmRules;
//			if (null != alarmRules) {
//				for (storm.dto.fault.FaultAlarmRule faultAlarmRule : alarmRules) {
//					System.out.print(faultAlarmRule);
//				}
//			}
//			System.out.println();
//		}
		
	}
}
