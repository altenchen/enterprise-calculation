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

import storm.util.dbconn.Conn;

/**
 * @author wza
 * 预警规则获取
 */
public class EarlyWarnsGetter {
    final static String ALL = "ALL";
    private static ConcurrentHashMap<String, EarlyWarn> earlyWarns;
    private static ConcurrentHashMap<String, Set<EarlyWarn>> typeWarns;
    //private static ConcurrentHashMap<String, Set<EarlyWarn>> typeAllWarns;
    private static ConcurrentHashMap<String, List<EarlyWarn>> typeAllWarnArrs;
    private static Conn conn ;
    private static List<Object[]>allEarlyWarns;
    private static boolean bulidSuccess;
    private static Lock lock;
    static {
        earlyWarns = new ConcurrentHashMap<String, EarlyWarn>();
        typeWarns = new ConcurrentHashMap<String, Set<EarlyWarn>>();
        //typeAllWarns = new ConcurrentHashMap<String, Set<EarlyWarn>>();
        typeAllWarnArrs = new ConcurrentHashMap<String, List<EarlyWarn>>();
        lock = new ReentrantLock();

        try {

            bulidSuccess = false;
            conn = new Conn();
            allEarlyWarns = conn.getAllWarns();
            initRules(allEarlyWarns);
            initTypeRules();
            bulidSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized static void rebulid(){

        try {
            if (!bulidSuccess) {
                return;
            }
            bulidSuccess = false;
            if (null == conn) {
                conn = new Conn();
            }
            allEarlyWarns = conn.getAllWarns();
            initRules(allEarlyWarns);
            initTypeRules();
            //typeAllWarns.clear();
            typeAllWarnArrs.clear();
            bulidSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void initRules(List<Object[]>allWarns){
        try {
            //ID, NAME, VEH_MODEL_ID, LEVELS, DEPEND_ID, L1_SEQ_NO, EXPR_LEFT, L2_SEQ_NO, EXPR_MID, R1_VAL, R2_VAL
            List<String> nowIds = null;
            if (null != allWarns && allWarns.size() > 0) {
                nowIds = new LinkedList<String>();
                for (Object[] rule : allWarns) {
                    if (null != rule) {

                        EarlyWarn warn = getEarlyByRule(rule);
                        if (null != warn && null != rule[0]) {
                            String id = (String)rule[0];
                            earlyWarns.put(id, warn);
                            nowIds.add(id);
                        }
                    }
                }
            }
            if (null ==nowIds || 0== nowIds.size()) {
                earlyWarns.clear();
            } else {
                if (earlyWarns.size() >0) {

                    Enumeration<String> allKeys = earlyWarns.keys();
                    List<String> needRemovekeys = new LinkedList<String>();
                    while (allKeys.hasMoreElements()) {
                        String key = (String) allKeys.nextElement();
                        if (! nowIds.contains(key)) {
                            needRemovekeys.add(key);
                        }
                    }

                    for (String key : needRemovekeys) {
                        earlyWarns.remove(key);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    static void initTypeRules(){
        typeWarns.clear();
        if (null != earlyWarns && earlyWarns.size() > 0) {
            Collection<EarlyWarn> warns = earlyWarns.values();
            for (EarlyWarn warn : warns) {
                String modelId = warn.vehModelId;
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

    private static EarlyWarn getEarlyByRule(Object[] rule){
        //ID, NAME, VEH_MODEL_ID, LEVELS, DEPEND_ID, L1_SEQ_NO, EXPR_LEFT, L2_SEQ_NO, EXPR_MID, R1_VAL, R2_VAL

        if (null == rule)
            return null;

        if (rule.length == 11) {
            String id = (null == rule[0])? null : (String)rule[0];
            id = (null == id || "".equals(id.trim())) ? null : id;

            String name = (null == rule[1])? null : (String)rule[1];
            name = (null == name || "".equals(name.trim())) ? null : name;

            String vehModelId = (null == rule[2])? null : (String)rule[2];
            vehModelId = (null == vehModelId || "".equals(vehModelId.trim())) ? null : vehModelId;

            int levels = (null == rule[3])? Integer.MIN_VALUE : (int)rule[3];

            String dependId = (null == rule[4])?null : (String)rule[4];
            dependId = (null == dependId || "".equals(dependId.trim())) ? null : dependId;

            String left1DataItem = (null == rule[5])?null : ""+rule[5];
            left1DataItem = (null == left1DataItem || "".equals(left1DataItem.trim())) ? null : left1DataItem;

            String leftExpression = (null == rule[6])?null : (String)rule[6];
            leftExpression = (null == leftExpression || "".equals(leftExpression.trim())) ? null :leftExpression;

            String left2DataItem = (null == rule[7])?null : ""+rule[7];
            left2DataItem = (null == left2DataItem || "".equals(left2DataItem.trim())) ? null : left2DataItem;

            String middleExpression = (null == rule[8])?null : ""+rule[8];
            middleExpression = (null == middleExpression || "".equals(middleExpression.trim())) ? null : middleExpression;

            float right1Value = (null == rule[9])? Float.MIN_VALUE : (float)rule[9];
            float right2Value = (null == rule[10])? Float.MIN_VALUE : (float)rule[10];

            if (null != id
                    && null != left1DataItem
                    && Float.MIN_VALUE != right1Value) {

                EarlyWarn warn = new EarlyWarn(id, name, vehModelId, levels, dependId, left1DataItem, leftExpression, left2DataItem, middleExpression, right1Value, right2Value);

//                EarlyWarn warn = new EarlyWarn(id, vehModelId, left1DataItem, leftExpression, left2DataItem, middleExpression, right1Value, right2Value);
                if (null != dependId)
                    warn.dependId = dependId;

                return warn;
            }
        }
        return null;
    }

    /**
     * 通用报警，使用所有车型，也包含国标通用报警
     * @return
     */
    public static Set<EarlyWarn> commonWarns(){

        return typeWarns.get(ALL);
    }

    //synchronized
    /*public static Set<EarlyWarn> allWarnsByType(String type){
        boolean buildSucc = bulidSuccessRetryTimes(150);
        if (!buildSucc) {
            return null;
        }

        Set<EarlyWarn> warns = typeAllWarns.get(type);
        if (null != warns) {
            return warns;
        }
        warns = getAllWarnRules(type);
        if (null != warns) {
            typeAllWarns.put(type, warns);
        }
        return warns;
    }*/
    /**
     *
     * @param type
     * @return
     */
    public static List<EarlyWarn> allWarnArrsByType(String type){
        boolean buildSucc = bulidSuccessRetryTimes(150);
        if (!buildSucc) {
            return null;
        }
        List<EarlyWarn> warns = typeAllWarnArrs.get(type);
        if (null != warns) {
            return warns;
        }
        try {

            lock.lock();
            Set<EarlyWarn> warnSet = getAllWarnRules(type);
            if (null != warnSet) {
                warns = new ArrayList<EarlyWarn>(warnSet.size());
                warns.addAll(warnSet);
                typeAllWarnArrs.put(type, warns);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            lock.unlock();
        }
        return warns;
    }

    /**
     * 所有的报警信息
     * @return
     */
    private synchronized static Set<EarlyWarn> getAllWarnRules(String type){
        Set<EarlyWarn> commonWarns = commonWarns();
        Set<EarlyWarn> customWarns = customWarns(type);
        if (null == commonWarns)
            return customWarns;

        if (null == customWarns)
            return commonWarns;

        Set<EarlyWarn> warns = new HashSet<EarlyWarn>();
        warns.addAll(commonWarns);
        warns.addAll(customWarns);
        return warns;
    }

    /**
     * 用户自建的预警规则
     * @param type
     * @return
     */
    public static Set<EarlyWarn> customWarns(String type){

        return typeWarns.get(type);
    }

    public static EarlyWarn getEarlyByDependId(String dependId) {
        return earlyWarns.get(dependId);
    }

    private static boolean bulidSuccessRetryTimes(int times){//尝试次数
        try {
            int count = 0;
            while (!bulidSuccess) {
                TimeUnit.MILLISECONDS.sleep(1);
                if (count >times) {
                    return false;
                }
                count ++;
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    public static void main(String[] args) {

        System.out.println("init conn");
    }

}
