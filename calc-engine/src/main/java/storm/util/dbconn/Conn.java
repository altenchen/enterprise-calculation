package storm.util.dbconn;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.dto.ExceptionSingleBit;
import storm.dto.FaultCodeByte;
import storm.dto.FaultCodeByteRule;
import storm.dto.FaultTypeSingleBit;
import storm.dto.alarm.CoefficientOffset;
import storm.dto.fence.EleFence;
import storm.handler.fence.input.AlarmRule;
import storm.handler.fence.input.Rule;
import storm.handler.fence.input.SpeedAlarmRule;
import storm.handler.fence.input.StopAlarmRule;
import storm.util.ConfigUtils;

import java.sql.*;
import java.util.*;

/**
 * @author wza
 * 数据库操作工具类
 */
public final class Conn {
    private static final Logger LOG = LoggerFactory.getLogger(Conn.class);

    /**
     * 创建数据库连接
     *
     * @return
     */
    @Nullable
    private static Connection getConn() {
        try {
            Class.forName(ConfigUtils.getSysDefine().getJdbcDriver());
        } catch (ClassNotFoundException e) {
            LOG.error("数据库驱动不存在", e);
        }

        final String url = ConfigUtils.getSysDefine().getJdbcUrl();
        final String username = ConfigUtils.getSysDefine().getJdbcUsername();
        final String password = ConfigUtils.getSysDefine().getJdbcPassword();

        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            LOG.warn("创建数据库连接失败.", e);
        }

        return null;
    }

    /**
     * @return 故障码报警规则
     */
    @NotNull
    public Collection<FaultCodeByteRule> getFaultAlarmCodes() {
        // <faultId, rules>
        final Map<String, FaultCodeByteRule> result = new TreeMap<>();

        final List<String[]> rules = getFaultRuleCodeObjects();

        final byte fault_id = 0;
        final byte fault_type = 1;
        final byte analyze_type = 2;
        final byte model_num = 3;
        final byte exception_type = 4;
        final byte exception_id = 5;
        final byte exception_code = 6;
        final byte response_level = 7;

        int count = 0;
        final Set<String> codeIds = new HashSet<>();

        for (String[] objs : rules) {
            ++count;

            final String faultId = objs[fault_id];
            if (StringUtils.isBlank(faultId)) {
                LOG.warn("空白的故障码.");
                continue;
            }

            final String faultType = objs[fault_type];
            if (StringUtils.isBlank(faultType)) {
                LOG.warn("故障码 {} 空白的故障码类型.", faultType);
                continue;
            }

            final String analyzeType = objs[analyze_type];
            if (StringUtils.isBlank(analyzeType)) {
                LOG.warn("故障码 {} 无效的解析方式 {}.", faultType, analyzeType);
                continue;
            }
            // 解析方式 1-按字节, 2-按位; 这里只处理按字节解析的情况
            if (!"1".equals(analyzeType)) {
                LOG.warn("故障码 {} 无效的按值解析解析方式 {}.", faultType, analyzeType);
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
            // TODO: 加上车型处理

            // 异常类型, 1-正常码, 2-异常码
            final int exceptionType;
            try {
                exceptionType = Integer.parseInt(objs[exception_type]);
            } catch (NumberFormatException ex) {
                ex.printStackTrace();
                LOG.error("故障码 {} 无效的异常类型[{}].", faultType, objs[exception_type]);
                continue;
            }
            if (exceptionType != 0 && exceptionType != 1) {
                LOG.error("故障码 {} 错误的异常类型[{}].", faultType, exceptionType);
                continue;
            }

            String exceptionId = objs[exception_id];
            if (StringUtils.isBlank(exceptionId)) {
                LOG.error("故障码 {} 空白的异常码Id.", faultType);
                continue;
            }

            String exceptionCode = objs[exception_code];
            if (StringUtils.isBlank(exceptionCode)) {
                LOG.error("故障码 {} 异常码 {}: 空白的异常码值.", faultType, exceptionId);
                continue;
            }
            // 数据库没有给十六进制数据库添加0x前缀, 则补上前缀
            if (!StringUtils.startsWithAny(exceptionCode, new String[]{"0x", "0X"})) {
                exceptionCode = "0x" + exceptionCode;
            }
            if (!NumberUtils.isNumber(exceptionCode)) {
                LOG.error("故障码 {} 异常码 {} 无效的异常码值 {}.", faultType, exceptionId, exceptionCode);
                continue;
            }

            final int responseLevel;
            try {
                responseLevel = Integer.parseInt(objs[response_level]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                LOG.error("故障码 {} 异常码 {} 错误的告警等级 {}.", faultType, exceptionId, objs[response_level]);
                continue;
            }

            if (codeIds.contains(exceptionId)) {
                LOG.warn("重复的异常码 {}", exceptionId);
                continue;
            }
            codeIds.add(exceptionId);

            final FaultCodeByteRule ruleCode = result.getOrDefault(faultId, new FaultCodeByteRule(faultId, faultType, modelNum));
            result.put(faultId, ruleCode);

            final FaultCodeByte faultCode = new FaultCodeByte(
                exceptionId,
                exceptionCode,
                responseLevel,
                exceptionType,
                    faultId);
            ruleCode.addFaultCode(faultCode);

        }
        LOG.info("更新获取到 {} 条按值解析故障码规则, 其中 {} 条有效.", count, codeIds.size());

        return result.values();
    }


    /**
     * 获取车辆车型关系
     *
     * @return Key-车辆Id, Value-车型Id
     */
    public Map<String, String> getVehicleModel() {

        final Map<String, String> vmd = new TreeMap<>();

        String veh_model_sql = ConfigUtils.getSysParam().getVehModelSql();
        if (StringUtils.isBlank(veh_model_sql)) {
            return vmd;
        }

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            connection = getConn();
            if (null == connection) {
                LOG.warn("创建数据库连接失败");
                return vmd;
            }

            statement = connection.createStatement();
            resultSet = statement.executeQuery(veh_model_sql);

            while (resultSet.next()) {

                // 车辆Id
                final String vid = resultSet.getString(1);
                if (StringUtils.isBlank(vid)) {
                    continue;
                }

                final String mid = resultSet.getString(2);
                if (StringUtils.isBlank(mid)) {
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
    public Map<String, Map<String, FaultTypeSingleBit>> getFaultSingleBitRules() {
        LOG.info("开始更新按位解析故障码规则");

        // 一个故障类型可以对应多个故障码
        // <fault_type, <faultId, fault>>
        final Map<String, Map<String, FaultTypeSingleBit>> faultTypes = new HashMap<>();

        String alarm_code_bit_sql = ConfigUtils.getSysParam().getAlarmCodeBitSql();
        if (StringUtils.isBlank(alarm_code_bit_sql)) {
            LOG.info("按位解析故障码查询语句为空.");
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
                LOG.warn("创建数据库连接失败");
                return faultTypes;
            }

            statement = connection.createStatement();
            analyzeBitResult = statement.executeQuery(alarm_code_bit_sql);
            //analyzeValueResult = statement.executeQuery(alarm_code_sql);

            // exception_id, exception
            final Map<String, ExceptionSingleBit> faultExceptionsCache = new HashMap<>();

            int count = 0;
            while (analyzeBitResult.next()) {

                ++count;
                LOG.trace("开始解析第[{}]条规则", count);

                // 故障码Id
                final String fault_id = analyzeBitResult.getString(1);
                if (StringUtils.isBlank(fault_id)) {
                    LOG.warn("空白的故障码.");
                    continue;
                }

                // 故障码类型(内部协议标号)
                final String fault_type = analyzeBitResult.getString(2);
                if (StringUtils.isBlank(fault_id)) {
                    LOG.warn("故障码 {} 空白的故障码类型.", fault_id);
                    continue;
                }

                // 解析方式 1-按字节, 2-按位
                final String analyze_type = analyzeBitResult.getString(3);
                if (!NumberUtils.isDigits(analyze_type)) {
                    LOG.warn("故障码 {} 无效的解析方式 {}.", fault_id, analyze_type);
                    continue;
                }

                final boolean isAnalyzeByBit = "2".equals(analyze_type);

                // 目前只处理按1位解析规则, 其它的走老规则
                if (isAnalyzeByBit) {

                    // 位长, 目前固定为1
                    final String param_length = StringUtils.defaultIfEmpty(
                            analyzeBitResult.getString(4),
                            "1");
                    if (!NumberUtils.isDigits(param_length)) {
                        LOG.warn("故障码 {} 按位解析方式下, 无效的位长类型 {}.", fault_id, param_length);
                        continue;
                    }

                    // 目前只处理按1位解析规则, 其它的走老规则
                    if (!"1".equals(param_length)) {
                        LOG.warn("故障码 {} 按位解析目前只支持位长1, 配置 {} 暂不支持.", fault_id, param_length);
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
                    if (!NumberUtils.isDigits(start_point)) {
                        LOG.warn("故障码 {} 按位解析方式下, 无效的起始位偏移量 {}.", fault_id, start_point);
                        continue;
                    }

                    // 异常码Id
                    final String exception_id = analyzeBitResult.getString(7);
                    if (StringUtils.isBlank(exception_id)) {
                        LOG.warn("故障码 {} 按位解析方式下, 空白的异常码Id.", fault_id);
                        continue;
                    }

                    final Short faultOffset;
                    try {
                        faultOffset = Short.decode(start_point);
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                        LOG.warn("故障码 {} 异常码 {} 起始位偏移量格式错误: {}", fault_id, exception_id, e.getLocalizedMessage());
                        continue;
                    }

                    // 异常码码值, 因为目前固定位长为1, 所以码值也只有1了
                    final String exception_code = analyzeBitResult.getString(8);
                    if (!NumberUtils.isDigits(exception_code)) {
                        LOG.warn("故障码 {} 按位解析方式下, 无效的异常码码值 {}.", fault_id, "exception_code");
                        continue;
                    }

                    // 时间阈值
                    final String time_threshold = StringUtils.defaultIfEmpty(
                            analyzeBitResult.getString(9),
                            "0");
                    if (!NumberUtils.isDigits(time_threshold)) {
                        LOG.warn("故障码 {} 按位解析方式下, 无效的时间阈值 {}.", fault_id, time_threshold);
                        continue;
                    }

                    final int lazy;
                    try {
                        lazy = Integer.decode(time_threshold);
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                        LOG.warn("故障码 {} 异常码 {} 时间阈值格式错误: {}", fault_id, exception_id, e.getLocalizedMessage());
                        continue;
                    }

                    // 告警等级
                    final String response_level = StringUtils.defaultIfEmpty(
                            analyzeBitResult.getString(10),
                            "0");
                    if (!NumberUtils.isDigits(response_level)) {
                        LOG.warn("故障码 {} 按位解析方式下, 无效的告警等级 {}.", fault_id, response_level);
                        continue;
                    }

                    final byte level;
                    try {
                        level = Byte.decode(response_level);
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                        LOG.warn("故障码 {} 异常码 {} 告警等级格式错误 {}", fault_id, exception_id, e.getLocalizedMessage());
                        continue;
                    }

                    // 故障类型规则集合
                    final Map<String, FaultTypeSingleBit> faultTypeRule = faultTypes.getOrDefault(
                            fault_type,
                            new HashMap<>()
                    );
                    faultTypes.put(fault_type, faultTypeRule);

                    // 故障码规则
                    final FaultTypeSingleBit faultRule = faultTypeRule.getOrDefault(
                            fault_id,
                            new FaultTypeSingleBit(
                                    fault_id,
                                    fault_type,
                                    analyze_type
                            )
                    );
                    faultTypeRule.put(fault_id, faultRule);

                    if (faultExceptionsCache.containsKey(exception_id)) {
                        LOG.warn("重复的异常码 {}.", exception_id);
                        continue;
                    }

                    // 异常码规则
                    final ExceptionSingleBit exceptionRule = new ExceptionSingleBit(exception_id, faultOffset, lazy, level, fault_id);
                    faultExceptionsCache.put(exceptionRule.exceptionId, exceptionRule);

                    // 将异常关联到适用车型, 空字符串表示默认车型
                    final String[] vehicleModels = ArrayUtils.isEmpty(model_num) ? new String[]{""} : model_num;
                    for (final String vehicleModel : vehicleModels) {
                        final Map<String, ExceptionSingleBit> exceptions =
                                faultRule.vehExceptions.getOrDefault(
                                        vehicleModel,
                                        new HashMap<>());
                        faultRule.vehExceptions.put(vehicleModel, exceptions);
                        if (exceptions.containsKey(exceptionRule.exceptionId)) {
                            LOG.warn("故障码 {} 车型 {} 重复的异常码 {}.", faultRule.faultId, vehicleModel, exceptionRule.exceptionId);
                            continue;
                        }
                        exceptions.put(exceptionRule.exceptionId, exceptionRule);
                    }
                }
            }
            LOG.info("更新获取到 {} 条按位解析故障码规则, 其中 {} 条有效.", count, faultExceptionsCache.size());


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
            LOG.warn("更新按位解析故障码规则异常", e);
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
        LOG.info("开始更新按值解析故障码规则");

        final List<String[]> rules = new LinkedList<>();

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        String alarm_code_sql = ConfigUtils.getSysParam().getAlarmCodeSql();
        try {
            if (StringUtils.isEmpty(alarm_code_sql)) {
                LOG.info("按值解析故障码查询语句为空.");
                return rules;
            }
            if (null == connection || connection.isClosed()) {
                connection = getConn();
            }
            if (null == connection) {
                LOG.warn("创建数据库连接失败");
                return rules;
            }

            statement = connection.createStatement();
            resultSet = statement.executeQuery(alarm_code_sql);
            while (resultSet.next()) {
                String[] rule = new String[]{
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
            LOG.warn("更新按值解析故障码规则异常", e);
        } finally {
            close(resultSet, statement, connection);
        }
        return rules;
    }

    /**
     * 预警规则
     *
     * @return
     */
    @Nullable
    public List<Object[]> getAllWarns() {
        List<Object[]> rules = null;
        Connection conn = null;
        Statement s = null;
        ResultSet rs = null;
        String early_warning_sql = ConfigUtils.getSysParam().getEarlyWarningSql();
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
            while (rs.next()) {
                Object[] objects = new Object[]{rs.getString(1), rs.getString(2),
                        rs.getString(3), rs.getInt(4), rs.getString(5),
                        rs.getString(6), rs.getString(7), rs.getString(8),
                        rs.getString(9), rs.getFloat(10), rs.getFloat(11)};
                rules.add(objects);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(rs, s, conn);
        }
        return rules;
    }

    public static void buildPlatformAlarm() {

    }

    /**
     * 偏移系数自定义数据项
     *
     * @return
     */
    @Nullable
    public static List<CoefficientOffset> getAllCoefOffset() {

        Connection conn = null;
        Statement s = null;
        ResultSet rs = null;

        String item_coef_offset_sql = ConfigUtils.getSysParam().getItemCoefOffsetSql();
        try {
            if (StringUtils.isEmpty(item_coef_offset_sql)) {
                return null;
            }
            conn = getConn();
            if (null == conn) {
                return null;
            }
            s = conn.createStatement();
            rs = s.executeQuery(item_coef_offset_sql);

            final List<CoefficientOffset> rules = new LinkedList<>();
            while (rs.next()) {

                final String sequencerNumber = rs.getString(1);

                final int isArray = rs.getInt(2);

                final double factor = NumberUtils.toDouble(rs.getString(3), 1);

                final double offset = rs.getDouble(4);

                final int isCustom = rs.getInt(5);

//                final CoefficientOffset coefficientOffset = new CoefficientOffset(
//                    // SEQ_NO, 序号
//                    sequencerNumber,
//                    // FACTOR, 系数
//                    factor,
//                    // OFFSET, 偏移值
//                    offset
//                );
//
//                rules.add(coefficientOffset);
            }
            return rules;

        } catch (SQLException e) {
            LOG.error("查询偏移系数自定义数据项异常", e);
        } finally {
            close(rs, s, conn);
        }

        return null;
    }

    /**
     * 返回 数据结构 vid -list EleFence
     *
     * @return
     */
    @Nullable
    public Map<String, List<EleFence>> vidFences() {

        List<String[]> vidFenceMap = getVidFenceMap();
        if (null == vidFenceMap || vidFenceMap.size() == 0) {
            return null;
        }
        Map<String, List<EleFence>> vidfences = new TreeMap<String, List<EleFence>>();
        try {
            Map<String, EleFence> fenceMap = fencesWithId();
//        Map<String, List<String>> vidfenceIds = new TreeMap<String, List<String>>();
            for (String[] strings : vidFenceMap) {
                if (null != strings && strings.length == 2) {
                    if (null != strings[0] && !"".equals(strings[0].trim())
                            && null != strings[1] && !"".equals(strings[1].trim())) {
                        String fenceId = strings[0].trim();
                        String vid = strings[1].trim();
//                    List<String> list = vidfenceIds.get(vid);
//                    if(null == list)
//                        list = new LinkedList<String>();
//                    if (!list.contains(fenceId))
//                        list.add(fenceId);

                        List<EleFence> fences = vidfences.get(vid);
                        EleFence fence = fenceMap.get(fenceId);
                        if (null == fences) {
                            fences = new LinkedList<EleFence>();
                        }
                        if (null != fence && !fences.contains(fence)) {
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

    @Nullable
    public Map<String, EleFence> fencesWithId() {
        try {
            List<EleFence> fences = getFences();
            List<Map<String, String>> rules = getRulesMap();
            Map<String, EleFence> fenceMap = null;
            if (null == rules) {
                return fenceMap;
            }
            Map<String, List<Rule>> ruleMap = groupRulesById(rules);
            fenceMap = new TreeMap<String, EleFence>();
            if (null != fences && fences.size() > 0) {
                for (EleFence eleFence : fences) {
                    String fenceId = eleFence.id;
                    List<Rule> fenceRules = ruleMap.get(fenceId);
                    if (null != fenceRules && fenceRules.size() > 0) {
                        eleFence.rules = fenceRules;
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

    @Nullable
    private List<EleFence> getFences() {
        List<EleFence> fences = null;
        Connection conn = null;
        Statement s = null;
        ResultSet rs = null;
        String fence_sql = ConfigUtils.getSysParam().getFenceSql();
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
            while (rs.next()) {
                EleFence fence = new EleFence();
                fence.id = rs.getString(1);
                fence.name = rs.getString(2);
                fence.type = rs.getString(3);
                fence.timesegs = rs.getString(4) + "|" + rs.getString(5);
                fence.pointRange = rs.getString(6);
                fence.periodValidtime = rs.getString(7);
                fence.status = "1";
                fence.build();
                fences.add(fence);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(rs, s, conn);
        }
        return fences;
    }

    @Nullable
    private List<String[]> getVidFenceMap() {
        List<String[]> rules = null;
        Connection conn = null;
        Statement s = null;
        ResultSet rs = null;
        String fence_vid_sql = ConfigUtils.getSysParam().getFenceVidSql();
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
            while (rs.next()) {
                String[] strings = new String[]{rs.getString(1), rs.getString(2)};
                rules.add(strings);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(rs, s, conn);
        }
        return rules;
    }

    @Nullable
    private List<Map<String, String>> getRulesMap() {
        List<Map<String, String>> rules = null;
        Connection conn = null;
        Statement s = null;
        ResultSet rs = null;
        String fence_rule_only_sql = ConfigUtils.getSysParam().getFenceRuleOnlySql();
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
            while (rs.next()) {
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
            close(rs, s, conn);
        }
        return rules;
    }

    @Contract("null -> null")
    private Map<String, List<Rule>> groupRulesById(List<Map<String, String>> rules) {
        if (null == rules) {
            return null;
        }

        try {
            Map<String, List<Rule>> maps = new TreeMap<String, List<Rule>>();
            for (Map<String, String> map : rules) {
                if (null != map && map.size() > 0) {
                    String fenceId = map.get("fenceId");
                    String alarmTypeCode = map.get("alarmTypeCode");
                    List<Rule> list = maps.get(fenceId);
                    if (null == list) {
                        list = new LinkedList<Rule>();
                    }
                    if ("0001".equals(alarmTypeCode)) {
                        SpeedAlarmRule rule = new SpeedAlarmRule();
                        rule.setCode(alarmTypeCode);
                        rule.speedType = AlarmRule.GT;
                        rule.speeds = new double[]{10 * toDouble(map.get("lowSpeed")), 10 * toDouble(map.get("highSpeed"))};
                        list.add(rule);
                    } else if ("0002".equals(alarmTypeCode)) {
                        SpeedAlarmRule rule = new SpeedAlarmRule();
                        rule.setCode(alarmTypeCode);
                        rule.speedType = AlarmRule.LT;
                        rule.speeds = new double[]{10 * toDouble(map.get("lowSpeed")), 10 * toDouble(map.get("highSpeed"))};
                        list.add(rule);
                    } else if ("0001,0002".equals(alarmTypeCode)) {
                        SpeedAlarmRule rule = new SpeedAlarmRule();
                        rule.speedType = AlarmRule.GLT;
                        rule.setCode(alarmTypeCode);
                        rule.speeds = new double[]{10 * toDouble(map.get("lowSpeed")), 10 * toDouble(map.get("highSpeed"))};
                        list.add(rule);
                    } else if ("0009".equals(alarmTypeCode)) {
                        StopAlarmRule rule = new StopAlarmRule();
                        rule.stopType = AlarmRule.IN;
                        int stopTime = toInt(map.get("stopTime"));
                        if (stopTime > 0) {
                            rule.stopTime = stopTime * 60;
                        }
                        rule.setCode(alarmTypeCode);
                        list.add(rule);
                    } else if ("0010".equals(alarmTypeCode)) {
                        StopAlarmRule rule = new StopAlarmRule();
                        rule.stopType = AlarmRule.OUT;
                        int stopTime = toInt(map.get("stopTime"));
                        if (stopTime > 0) {
                            rule.stopTime = stopTime * 60;
                        }
                        rule.setCode(alarmTypeCode);
                        list.add(rule);
                    } else if ("0009,0010".equals(alarmTypeCode)) {
                        StopAlarmRule rule = new StopAlarmRule();
                        rule.stopType = AlarmRule.INOUT;
                        int stopTime = toInt(map.get("stopTime"));
                        if (stopTime > 0) {
                            rule.stopTime = stopTime * 60;
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

    private int toInt(String str) {
        return (int) NumberUtils.toDouble(str, -1);
    }

    private double toDouble(String str) {
        return NumberUtils.toDouble(str, -1);
    }

    @Nullable
    public List<Map<String, Object>> get(String sql, String[] filedName) {
        List<Map<String, Object>> list = null;
        Connection conn = null;
        Statement s = null;
        ResultSet rs = null;
        try {
            conn = getConn();
            if (null == conn) {
                return null;
            }
            if (null == filedName || filedName.length < 1) {
                return list;
            }
            list = new LinkedList<>();
            s = conn.createStatement();
            rs = s.executeQuery(sql);
            while (rs.next()) {
                Map<String, Object> map = new TreeMap<String, Object>();
                for (int i = 0; i < filedName.length; i++) {
                    if (StringUtils.isNotBlank(filedName[i])) {
                        map.put(filedName[i], rs.getObject(i + 1));
                    }
                }
                list.add(map);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(rs, s, conn);
        }
        return list;
    }

    /**
     * 释放系统资源
     */
    private static void close(AutoCloseable... resources) {
        if (ArrayUtils.isNotEmpty(resources)) {
            for (AutoCloseable resource : resources) {
                if (null != resource) {
                    try {
                        resource.close();
                    } catch (Exception e) {
                        LOG.warn("释放资源异常.", e);
                    }
                }
            }
        }
    }
}
