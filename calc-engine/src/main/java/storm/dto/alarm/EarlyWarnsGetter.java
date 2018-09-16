package storm.dto.alarm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.util.dbconn.Conn;

/**
 * @author wza
 * 预警规则获取
 */
public class EarlyWarnsGetter {

    private static final Logger LOG = LoggerFactory.getLogger(EarlyWarnsGetter.class);

    final static String ALL = "ALL";

    /**
     * 预警规则 <ruleId, rule>
     */
    private static final ConcurrentHashMap<String, EarlyWarn> earlyWarns = new ConcurrentHashMap<>();

    /**
     * 按车型预警规则 <车型, 预警规则>, 通用规则类型为ALL
     */
    private static final ConcurrentHashMap<String, Set<EarlyWarn>> typeWarns = new ConcurrentHashMap<>();

    /**
     * 按车型预警规则, 包含通用规则.
     */
    private static final ConcurrentHashMap<String, List<EarlyWarn>> typeAllWarnArrs = new ConcurrentHashMap<>();

    /**
     * typeAllWarnArrs 同步锁
     */
    private static final Lock lock = new ReentrantLock();

    /**
     * 查询数据库的封装类
     */
    private static final Conn conn = new Conn();

    /**
     * 从数据库查出来的待解析预警规则
     */
    private static List<Object[]> allEarlyWarns;

    private static boolean buildSuccess = true;

    static {
        rebuild();
    }

    public synchronized static void rebuild() {

        try {

            if (!buildSuccess) {
                LOG.info("平台报警规则正在始化, 跳过.");
                return;
            }

            LOG.info("平台报警规则初始化开始.");
            buildSuccess = false;

            allEarlyWarns = conn.getAllWarns();
            if(null != allEarlyWarns) {
                LOG.info("从数据库获取到平台报警规则[{}]条", allEarlyWarns.size());
            } else {
                LOG.info("从数据库获取到平台报警规则[0]条");
            }

            // 解析预警规则
            initRules(allEarlyWarns);
            // 将规则按车型分组
            initTypeRules();

            // 清空按车型预警规则
            typeAllWarnArrs.clear();

            buildSuccess = true;
            LOG.info("平台报警规则初始化完毕.");

        } catch (Exception e) {
            LOG.warn("平台报警规则初始化异常", e);
        }
    }

    /**
     * 解析预警规则集合, 将规则更新到earlyWarns, 并清除earlyWarns中没有被更新到的过时规则.
     * @param allWarns
     */
    private static void initRules(List<Object[]> allWarns) {
        try {
            //ID, NAME, VEH_MODEL_ID, LEVELS, DEPEND_ID, L1_SEQ_NO, EXPR_LEFT, L2_SEQ_NO, EXPR_MID, R1_VAL, R2_VAL
            List<String> nowIds = null;
            if (null != allWarns && allWarns.size() > 0) {
                nowIds = new LinkedList<String>();
                for (Object[] rule : allWarns) {
                    if (null != rule) {

                        EarlyWarn warn = getEarlyByRule(rule);
                        if (null != warn && null != rule[0]) {
                            String id = (String) rule[0];
                            // 将解析后的预警规则存储到earlyWarns并在nowIds中标记
                            earlyWarns.put(id, warn);
                            nowIds.add(id);
                        }
                    }
                }
            }
            if (null == nowIds || 0 == nowIds.size()) {
                earlyWarns.clear();
            } else {
                if (earlyWarns.size() > 0) {

                    Enumeration<String> allKeys = earlyWarns.keys();
                    List<String> needRemovekeys = new LinkedList<String>();
                    while (allKeys.hasMoreElements()) {
                        String key = (String) allKeys.nextElement();
                        if (!nowIds.contains(key)) {
                            // 标记已被删除的预警规则
                            needRemovekeys.add(key);
                        }
                    }

                    for (String key : needRemovekeys) {
                        // 移除已被删除的预警规则
                        earlyWarns.remove(key);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("解析预警规则集合异常", e);
        }

    }

    /**
     * 将规则按车型分组, 存储到 typeWarns 中
     */
    private static void initTypeRules() {
        typeWarns.clear();
        if (null != earlyWarns && earlyWarns.size() > 0) {
            Collection<EarlyWarn> warns = earlyWarns.values();
            for (EarlyWarn warn : warns) {
                String modelId = warn.vehicleModelId;
                if (warn.isAllCommon) {
                    modelId = ALL;
                }
                Set<EarlyWarn> allCommons = typeWarns.get(modelId);
                if (null == allCommons) {
                    allCommons = new HashSet<EarlyWarn>();
                }
                allCommons.add(warn);
                typeWarns.put(modelId, allCommons);
            }
        }
    }

    /**
     * 解析单条预警规则
     * @param rule
     * @return
     */
    private static EarlyWarn getEarlyByRule(Object[] rule) {
        //ID, NAME, VEH_MODEL_ID, LEVELS, DEPEND_ID, L1_SEQ_NO, EXPR_LEFT, L2_SEQ_NO, EXPR_MID, R1_VAL, R2_VAL

        if (null == rule) {
            return null;
        }

        if (rule.length == 11) {
            String id = (null == rule[0]) ? null : (String) rule[0];
            id = (null == id || "".equals(id.trim())) ? null : id;

            String name = (null == rule[1]) ? null : (String) rule[1];
            name = (null == name || "".equals(name.trim())) ? null : name;

            String vehModelId = (null == rule[2]) ? null : (String) rule[2];
            vehModelId = (null == vehModelId || "".equals(vehModelId.trim())) ? null : vehModelId;

            int levels = (null == rule[3]) ? Integer.MIN_VALUE : (int) rule[3];

            String dependId = (null == rule[4]) ? null : (String) rule[4];
            dependId = (null == dependId || "".equals(dependId.trim())) ? null : dependId;

            String left1DataItem = (null == rule[5]) ? null : "" + rule[5];
            left1DataItem = (null == left1DataItem || "".equals(left1DataItem.trim())) ? null : left1DataItem;

            String leftExpression = (null == rule[6]) ? null : (String) rule[6];
            leftExpression = (null == leftExpression || "".equals(leftExpression.trim())) ? null : leftExpression;

            String left2DataItem = (null == rule[7]) ? null : "" + rule[7];
            left2DataItem = (null == left2DataItem || "".equals(left2DataItem.trim())) ? null : left2DataItem;

            String middleExpression = (null == rule[8]) ? null : "" + rule[8];
            middleExpression = (null == middleExpression || "".equals(middleExpression.trim())) ? null : middleExpression;

            float right1Value = (null == rule[9]) ? Float.MIN_VALUE : (float) rule[9];
            float right2Value = (null == rule[10]) ? Float.MIN_VALUE : (float) rule[10];

            if (null != id
                && null != left1DataItem
                && Float.MIN_VALUE != right1Value) {

                EarlyWarn warn = new EarlyWarn(id, name, vehModelId, levels, dependId, left1DataItem, leftExpression, left2DataItem, middleExpression, right1Value, right2Value);

                if (null != dependId) {
                    warn.dependId = dependId;
                }

                return warn;
            }
        }
        return null;
    }

    /**
     * 通用报警，使用所有车型，也包含国标通用报警
     *
     * @return
     */
    public static Set<EarlyWarn> commonWarns() {

        return typeWarns.get(ALL);
    }

    /**
     * @param vehType 车辆类型, 预警用于匹配约束条件
     * @return 预警规则列表
     */
    @Nullable
    public static List<EarlyWarn> allWarnArrsByType(String vehType) {
        boolean buildSucc = buildSuccessRetryTimes(150);
        if (!buildSucc) {
            return null;
        }
        List<EarlyWarn> warns = typeAllWarnArrs.get(vehType);
        if (null != warns) {
            return warns;
        }
        try {
            // 多个线程来到这里, 则每个线程都会执行一次下面的逻辑.
            lock.lock();
            Set<EarlyWarn> warnSet = getAllWarnRules(vehType);
            if (null != warnSet) {
                warns = new ArrayList<>(warnSet.size());
                warns.addAll(warnSet);
                typeAllWarnArrs.put(vehType, warns);
            }
        } catch (Exception e) {
            LOG.error("获取特定车型的预警规则异常", e);
        } finally {

            lock.unlock();
        }
        return warns;
    }

    /**
     * 所有的报警信息
     *
     * @return
     */
    private synchronized static Set<EarlyWarn> getAllWarnRules(String type) {
        Set<EarlyWarn> commonWarns = commonWarns();
        Set<EarlyWarn> customWarns = customWarns(type);
        if (null == commonWarns) {
            return customWarns;
        }

        if (null == customWarns) {
            return commonWarns;
        }

        Set<EarlyWarn> warns = new HashSet<EarlyWarn>();
        warns.addAll(commonWarns);
        warns.addAll(customWarns);
        return warns;
    }

    /**
     * 用户自建的预警规则
     *
     * @param type
     * @return
     */
    public static Set<EarlyWarn> customWarns(String type) {

        return typeWarns.get(type);
    }

    public static EarlyWarn getEarlyByDependId(String dependId) {
        return earlyWarns.get(dependId);
    }

    /**
     *
     * @param times 尝试次数
     * @return
     */
    private static boolean buildSuccessRetryTimes(int times) {
        try {
            int count = 0;
            while (!buildSuccess) {
                TimeUnit.MILLISECONDS.sleep(1);
                if (count > times) {
                    return false;
                }
                count++;
            }
            return true;
        } catch (Exception e) {
            LOG.error("等待预警规则同步完成重试异常", e);
        }
        return false;
    }
}
