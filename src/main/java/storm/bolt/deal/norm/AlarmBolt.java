package storm.bolt.deal.norm;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.alibaba.fastjson.JSON;
import com.sun.jersey.core.util.Base64;

import storm.protocol.CommandType;
import storm.protocol.SUBMIT_LINKSTATUS;
import storm.protocol.SUBMIT_LOGIN;
import storm.system.DataKey;
import storm.util.NumberUtils;
import storm.util.ObjectUtils;
import storm.dto.alarm.CoefOffset;
import storm.dto.alarm.CoefOffsetGetter;
import storm.dto.alarm.EarlyWarn;
import storm.dto.alarm.EarlyWarnsGetter;
import storm.dto.alarm.WarnRecord;
import storm.dto.alarm.WarnningRecorder;
import storm.system.SysDefine;

@SuppressWarnings("Duplicates")
public class AlarmBolt extends BaseRichBolt {
    /**
     * 
     */
    private static final long serialVersionUID = 1720001L;

    private OutputCollector collector;

    private Map<String, List<String>> filterMap ;
    // 连续多少条报警才发送通知
    private static int alarmNum = 10;
    private static int printLevel;
    
    private static String vehAlarmTopic;
    private static String vehAlarmStoreTopic;
    private static ThreadLocal<SimpleDateFormat> formatlocal = new ThreadLocal<SimpleDateFormat>();
    public static final String DATA_FORMAT = "yyyyMMddHHmmss";
    private Map<String, String> alarmMap;
    private Map<String, String> vehDataMap;
    private Map<String, String> vid2Alarm;
    private Map<String, String> vid2AlarmEnd;
    private Map<String, String> vid2AlarmInfo;
    private Map<String, Set<String>> vidAlarmIds;
    private Map<String, Map<String, String>>lastCache;
    private long flushtime=300;//默认300秒同步数据库新建规则
    private static long oncesend = 60;//每隔多少时间推送ES一次,默认一分钟，60毫秒。如果负数或者0代表实时推送;
    private static long onlinetime = 180 * 1000 ;//离线超时时间
    private WarnningRecorder recorder;
    int buffsize = 5000000;
    LinkedBlockingQueue<String> alives = new LinkedBlockingQueue<String>(buffsize);
    Set<String> aliveSet = new HashSet<String>(buffsize/5);
    LinkedBlockingQueue<String> needListenAlarms = new LinkedBlockingQueue<String>(buffsize);
    Set<String> needListenAlarmSet = new HashSet<String>(buffsize/5);
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        context.getThisComponentId();
        context.getThisTaskId();
        filterMap = new HashMap<String, List<String>>();
        new DecimalFormat("##0.000000");

        printLevel = Integer.valueOf(stormConf.get("print.log").toString());
        vehAlarmTopic = stormConf.get("kafka.topic.alarm").toString();
        vehAlarmStoreTopic = stormConf.get("kafka.topic.alarmstore").toString();
        recorder =  new WarnningRecorder();
        try {
            Object alarmObject = stormConf.get("alarm.continue.counts");
            if (null !=alarmObject) {
                alarmNum = Integer.parseInt(NumberUtils.stringNumber(alarmObject.toString()));
            }
            Object oncetime = stormConf.get("es.send.time");
            if(null != oncetime)
                oncesend = Long.valueOf(oncetime.toString());
            flushtime=Long.parseLong(stormConf.get("db.cache.flushtime").toString());
            
            Object offli=stormConf.get("redis.offline.time");
            if(null != offli)
                onlinetime=1000*Long.valueOf(offli.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        alarmMap= new HashMap<String,String>();
        vehDataMap= new HashMap<String,String>();
        vid2Alarm = new HashMap<String, String>();//车辆报警信息缓存(vid----是否报警_最后报警时间)
        vid2AlarmEnd = new HashMap<String, String>();//车辆报警信息缓存(vid----报警结束次数)
        vid2AlarmInfo = new HashMap<String, String>();
        vidAlarmIds = new HashMap<String, Set<String>>();
        lastCache = new HashMap<String, Map<String, String>>();
        try {
            
            class AllSendClass implements Runnable{

                @Override
                public void run() {
                    int count = 0;
                    
                    try {
                        if (alives.size() > 0) {
                            
                            String keyVid = alives.poll();
                            while (null != keyVid) {
                                aliveSet.remove(keyVid);
                                
                                Map<String, String> dat = lastCache.get(keyVid);
                                if (null != dat && dat.size() >0) {
                                    
                                    String lastUtc = dat.get(SysDefine.ONLINEUTC);
                                    if (null != lastUtc && !"".equals(lastUtc.trim())) {
                                        
                                        sendToNext(SysDefine.SYNES_GROUP,keyVid, dat);
                                        count++;
                                        if (count % 1000 ==0) {
                                            count = 1;
                                            TimeUnit.MILLISECONDS.sleep(1);
                                        }
                                    }
                                }
                                keyVid = alives.poll();
                            }
                        }
                        
                    } catch (Exception e) {
                        e.printStackTrace();
                    } 
                }
                
            }
            
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new AllSendClass(), 0, oncesend, TimeUnit.SECONDS);
        
            class RebulidClass implements Runnable{

                @Override
                public void run() {
                    try {
                        EarlyWarnsGetter.rebulid();
                        CoefOffsetGetter.rebuild();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                
            }
        
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new RebulidClass(), 0, flushtime, TimeUnit.SECONDS);
            
            class TimeOutClass implements Runnable{

                @Override
                public void run() {
                    try {
                        timeOutOver(onlinetime);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                
            }
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new TimeOutClass(), 0, flushtime, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals(SysDefine.SPLIT_GROUP)) {
            String vid = input.getString(0);
            Map<String, String> dat = (TreeMap<String, String>) input.getValue(1);
            if (printLevel >= 5) {
                System.out.println("Receive kafka message REALINFO-------------------------------MSG:" + JSON.toJSONString(dat));
            }

            if (!dat.containsKey(SysDefine.TIME)
                    || ObjectUtils.isNullOrEmpty(dat.get(SysDefine.TIME))) {
                return;
            }
            String type = dat.get(SysDefine.MESSAGETYPE);

            if (CommandType.SUBMIT_REALTIME.equals(type)
                    || (CommandType.SUBMIT_LINKSTATUS.equals(type) && SUBMIT_LINKSTATUS.isOfflineNotice(dat.get(SUBMIT_LINKSTATUS.LINK_TYPE)))
                    || (CommandType.SUBMIT_LOGIN.equals(type)
                            && (dat.containsKey(SUBMIT_LOGIN.LOGOUT_SEQ)
                                    || dat.containsKey(SUBMIT_LOGIN.LOGOUT_TIME)))
                    ) {
                try {
                    processAlarm(dat, type);
                } catch (Exception e) {
                    System.out.println("软报警分析出错！map:" + JSON.toJSONString(dat));
                    e.printStackTrace();
                }
            }

            if (CommandType.SUBMIT_REALTIME.equals(type) || CommandType.SUBMIT_LOGIN.equals(type) || CommandType.SUBMIT_TERMSTATUS.equals(type) || CommandType.SUBMIT_CARSTATUS.equals(type)) {
                // hbase存储
//                sendAlarmKafka(SysDefine.VEH_ALARM_REALINFO_STORE,vehRealinfoStoreTopic, vid, JSON.toJSONString(dat));
                try {
                    String string = vid2Alarm.get(vid);
                    if (!ObjectUtils.isNullOrEmpty(string)) {
                        String[] alarmStr = string.split("_", 3);
                        dat.put(SysDefine.ISALARM, new String(alarmStr[0]));
                        dat.put(SysDefine.ALARMUTC, new String(alarmStr[1]));
                        alarmStr=null;
                    }
                 // redis存储
                    /**
                     * 当完全替换saveservice以后 打开
                     */
//                    CTFOUtils.getDefaultCTFOCacheTable().addHash(vid, dat);
                    
                } catch (Exception e) {
                    System.out.println("实时数据redis存储出错！map:" + JSON.toJSONString(dat));
                }
            } else if (CommandType.SUBMIT_LINKSTATUS.equals(type)) { // 车辆链接状态 TYPE：1上线，2心跳，3离线
                Map<String, String> linkmap = new TreeMap<String, String>();
                if ("1".equals(dat.get("TYPE"))) {
                    linkmap.put(SysDefine.ISONLINE, "1");
                } else if ("3".equals(dat.get("TYPE"))) {
                    linkmap.put(SysDefine.ISONLINE, "0");
                    linkmap.put(SysDefine.ISALARM, "0");
                }
                linkmap.put(SysDefine.ONLINEUTC, System.currentTimeMillis() + ""); // 增加utc字段，插入系统时间
                /**
                 * 当完全替换saveservice以后 打开
                 */
//                CTFOUtils.getDefaultCTFOCacheTable().addHash(vid, linkmap);
                dat.putAll(linkmap);
            }

            if (CommandType.SUBMIT_REALTIME.equals(type)
                    || CommandType.SUBMIT_LOGIN.equals(type)
                    || CommandType.SUBMIT_LINKSTATUS.equals(type)
                    || CommandType.SUBMIT_TERMSTATUS.equals(type)
                    || CommandType.SUBMIT_CARSTATUS.equals(type)){
                
                lastCache.put(vid, dat);
                if (! aliveSet.contains(vid)) {
                    aliveSet.add(vid);
                    alives.offer(vid);
                }

                //实时发送(不缓存)到 实时含告警的报文信息 到es同步服务
                //sendToNext(SysDefine.SYNES_GROUP,vid, dat);
            }
            if (CommandType.SUBMIT_REALTIME.equals(type)) {
                try {
                    String veh2000=dat.get("2000");
                    if (!ObjectUtils.isNullOrEmpty(veh2000)) {
                        String string=vehDataMap.get(vid);
                        if (null ==string || (string.compareTo(veh2000) < 0)) {
                            vehDataMap.put(vid, veh2000);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            dat=null;
        }else {
            System.out.println("Receive unknown kafka message-------------------StreamID:"+input.getSourceStreamId());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(SysDefine.VEH_ALARM, new Fields("TOPIC", DataKey.VEHICLE_ID, "VALUE"));
        declarer.declareStream(SysDefine.VEH_ALARM_REALINFO_STORE, new Fields("TOPIC", DataKey.VEHICLE_ID, "VALUE"));
        declarer.declareStream(SysDefine.FAULT_GROUP, new Fields(DataKey.VEHICLE_ID, "DATA"));
        declarer.declareStream(SysDefine.SYNES_GROUP, new Fields(DataKey.VEHICLE_ID, "DATA"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * 软报警处理
     * @param dataMap
     */
    private void warnning(Map<String, String> dataMap ,String type) {
        if(ObjectUtils.isNullOrEmpty(dataMap))
            return;
        String vid = dataMap.get(DataKey.VEHICLE_ID);
        String vType = dataMap.get("VTYPE");
        if (ObjectUtils.isNullOrEmpty(vid)
                || ObjectUtils.isNullOrEmpty(vType) ) 
            return;
        
        /**
         *<p>
         *此处是 结束的 心跳 已经 登出的命令，在调用方法的时候已经过滤了，<br/>
         *因此不需要再次判定是否下线
         * </p>
         */
        if(CommandType.SUBMIT_LINKSTATUS.equals(type)
                || CommandType.SUBMIT_LOGIN.equals(type)){
            try {
                sendOverAlarmMessage(vid);
            } catch (Exception e) {
                System.out.println("---自动发送结束报警异常！" + e);
            }
            return;
        }
        
        List<EarlyWarn> warns = EarlyWarnsGetter.allWarnArrsByType(vType);
        if (ObjectUtils.isNullOrEmpty(warns)) 
            return;

        try {
            int len = warns.size();
            for (int i = 0; i < len; i++) {
                EarlyWarn warn = warns.get(i);
                if (null == warn) 
                    continue;
                
                int ret = 0;
                if (null == warn.dependId) {
                    
                    ret = processSingleAlarm(vid,warn,dataMap);
                    sendAlarmMessage(ret, vid, warn, dataMap);
                    
                } else if (null != warn.dependId) {
                    EarlyWarn warndepend = EarlyWarnsGetter.getEarlyByDependId(warn.dependId);
                    ret = processSingleAlarm(vid,warn,dataMap);
                    if(ret==1){ //先判断父级约束是否成立，如果成立则继续判断子级约束
//                    ret = 0;
                        //String[] brr1 = alarm_arr[1].split(",");
                        if (null != warndepend) {
                            ret = processSingleAlarm(vid,warndepend,dataMap);
                        }
                    }
                    sendAlarmMessage(ret, vid, warndepend, dataMap);
                } else if (null != warn.earlyWarns
                        && warn.earlyWarns.size() >0) {
                    
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void statisticsWarn(int ret,String vid,EarlyWarn warn,Map<String,String> dataMap){
        if(null == warn)return;
        WarnRecord record = recorder.getWarnRecord(vid, warn.id);
        if (1 == ret) {
            if (null == record) {
                record = new WarnRecord(warn.id);
                record.setId("");
                record.setRealValue("");
            } 
            record.count("", 1);
            recorder.putRecord(vid, record);
        } else if (2 == ret) {
            if (null != record) {
                
            }
            
        } else if (0 == ret) {
            
        }
    }
    
    private void startWarnning(){
        
    }
    
    private void endWarnning(){
        
    }
    /**
     * 软报警处理
     * @param dataMap
     */
    private void processAlarm(Map<String, String> dataMap ,String type) {
        if(ObjectUtils.isNullOrEmpty(dataMap))
            return;
        String vid = dataMap.get(DataKey.VEHICLE_ID);
        String vType = dataMap.get("VTYPE");
        if (ObjectUtils.isNullOrEmpty(vid)
                || ObjectUtils.isNullOrEmpty(vType) ) 
            return;
        
        
        if(CommandType.SUBMIT_LINKSTATUS.equals(type)
                || CommandType.SUBMIT_LOGIN.equals(type)){
            try {
                sendOverAlarmMessage(vid);
            } catch (Exception e) {
                System.out.println("---自动发送结束报警异常！" + e);
            }
            return;
        }

        List<EarlyWarn> warns = EarlyWarnsGetter.allWarnArrsByType(vType);
        if (ObjectUtils.isNullOrEmpty(warns)) 
            return;
        try {
            int len = warns.size();
            for (int i = 0; i < len; i++) {
                EarlyWarn warn = warns.get(i);
                if (null == warn) 
                    continue;
                
                int ret = 0;
                if (null == warn.dependId) {
                    
                    ret = processSingleAlarm(vid,warn,dataMap);
                    sendAlarmMessage(ret, vid, warn, dataMap);
                    
                } else if (null != warn.dependId) {
                    EarlyWarn warndepend = EarlyWarnsGetter.getEarlyByDependId(warn.dependId);
                    ret = processSingleAlarm(vid,warn,dataMap);
                    if(ret==1){ //先判断父级约束是否成立，如果成立则继续判断子级约束
//                    ret = 0;
                        //String[] brr1 = alarm_arr[1].split(",");
                        if (null != warndepend) {
                            ret = processSingleAlarm(vid,warndepend,dataMap);
                        }
                    }
                    sendAlarmMessage(ret, vid, warndepend, dataMap);
                } else if (null != warn.earlyWarns
                        && warn.earlyWarns.size() >0) {
                    
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 车辆发离线通知，系统自动发送结束报警通知
     * @param vid
     */
    private void sendOverAlarmMessage(String vid) {
        if(filterMap.containsKey(vid)){
            String time = vid2Alarm.get(vid).split("_")[2];
            List<String> list = filterMap.get(vid);
            for(String filterId : list){
                //上条报警，本条不报警，说明是结束报警，发送结束报警报文
                String alarmId = alarmMap.get(vid+"#"+filterId);
                EarlyWarn warn = EarlyWarnsGetter.getEarlyByDependId(filterId);
                if (null ==warn) 
                    continue;
                
                String alarmName = warn.name;
                int alarmLevel = warn.levels;
                String left1 = warn.left1DataItem;
                
                //String alarmEnd = "VEHICLE_ID:"+vid+",ALARM_ID:"+alarmId+",STATUS:3,TIME:"+time+",CONST_ID:"+filterId;
                Map<String,Object> sendMsg = new TreeMap<String,Object>();
                sendMsg.put(DataKey.VEHICLE_ID, vid);
                sendMsg.put("ALARM_ID", alarmId);
                sendMsg.put("STATUS", 3);
                sendMsg.put("TIME", time);
                sendMsg.put("CONST_ID", filterId);
                String alarmEnd = JSON.toJSONString(sendMsg);
                        
                sendMsg.put("ALARM_NAME", alarmName);
                sendMsg.put("ALARM_LEVEL", alarmLevel);
                sendMsg.put("LEFT1", left1);
                String alarmhbase = JSON.toJSONString(sendMsg);
                //kafka存储
                sendAlarmKafka(SysDefine.VEH_ALARM,vehAlarmTopic,vid, alarmEnd);
                //hbase存储
                sendAlarmKafka(SysDefine.VEH_ALARM_REALINFO_STORE,vehAlarmStoreTopic, vid, alarmhbase);
                
//                sendMsg.put("COUNT",1234 );
//                sendMsg.put("ALARM_VAL", "xxxxxx");
//                sendMsg.put("UTC_TIME", getTime(time));
                
                //发送到 故障判断处理节点继续
//                sendToNext(SysDefine.FAULT_GROUP,vid, sendFault);
                //redis存储
                saveRedis(vid,"0",time);
                alarmMap.remove(vid+"#"+filterId);
            }
            filterMap.remove(vid);
        }
        //离线重置所有报警约束
        removeByVid(vid);
    }
    private int processSingleAlarm(String vid, EarlyWarn warn, Map<String, String> dataMap) {
        int ret =0;
        try {
            if(null != warn){
                String left1 = warn.left1DataItem; //左1数据项ID
                //偏移系数，
                CoefOffset coefOffset = CoefOffsetGetter.getCoefOffset(left1);
                String left1Value = dataMap.get(left1);
              //上传的实时数据包含左1字段 才进行预警判定
                if (ObjectUtils.isNullOrEmpty(left1Value)) 
                    return ret;
                boolean stringIsNum = NumberUtils.stringIsNumber(left1Value);
                
                if (null != coefOffset 
                        && 0 == coefOffset.type
                        && ! stringIsNum
                        ) 
                    return ret;
                if (null == coefOffset 
                        && ! stringIsNum)
                    return ret;
                
                int leftExp = Integer.valueOf(NumberUtils.stringNumber(warn.leftExpression));
                String left2 = warn.left2DataItem; //左2数据项ID
                int midExp = Integer.valueOf(NumberUtils.stringNumber(warn.middleExpression));
                double right1 = warn.right1Value;//Double.valueOf(NumberUtils.stringNumber(brr[8]));
                double right2 = warn.right2Value;//Double.valueOf(NumberUtils.stringNumber(brr[9]));
                if(ObjectUtils.isNullOrEmpty(left2)){   //左二字段为空，L2_ID为空  根据EXPR_MID，和R1_VAL, R2_VAL判断
                    
                    //不需要处理偏移和系数
                    if (null == coefOffset) {
                        double left1_value = Double.valueOf(NumberUtils.stringNumber(left1Value));
                        ret = diffMarkValid(left1_value,midExp,right1,right2); //判断是否软报警条件(true/false)
                    } else if(0 == coefOffset.type ) {
                        double left1_value = Double.valueOf(NumberUtils.stringNumber(left1Value));
                        left1_value = (left1_value - coefOffset.offset)/coefOffset.coef;
                        ret = diffMarkValid(left1_value,midExp,right1,right2); //判断是否软报警条件(true/false)
                    } else if(1 == coefOffset.type) {// 1代表是数据项是数组
                        //  判断:单体蓄电池电压值列表    7003 |温度值列表    7103
                        String[] arr = left1Value.split("\\|");
                        for(int i =0;i<arr.length;i++){
                            String arri = arr[i];
                            if (! ObjectUtils.isNullOrEmpty(arri)) {
                                
                                String v= new String(Base64.decode(new String(arri)),"GBK");
                                
                                if (v.contains(":")){
                                    String [] arr2m = v.split(":");
                                    if (arr2m.length ==2 
                                            && !ObjectUtils.isNullOrEmpty(arr2m[1])) {
                                        
                                        String[] arr2 = arr2m[1].split("_");
                                        for(int j=0;j<arr2.length;j++){
                                            double value = Double.parseDouble(NumberUtils.stringNumber(arr2[j]));
                                            value= (value - coefOffset.offset)/coefOffset.coef;
                                            ret = diffMarkValid(value,midExp,right1,right2); //判断是否软报警条件(true/false)
                                            if(ret==1) return ret;
                                        }
                                    }
                                }
                            }
                        }
                        
                    }
                    
                } else {
                    
                    String left2Value = dataMap.get(left2);
                    if (ObjectUtils.isNullOrEmpty(left2Value)) 
                        return ret;
                    
                    if(!left1.equals(left2)){ //L2_ID不为空， L1_ID  EXPR_LEFT  L2_ID
                        if (null != coefOffset && 1 == coefOffset.type) 
                            return ret;
                        
                        CoefOffset left2coefOffset = CoefOffsetGetter.getCoefOffset(left2);
                        if (null != left2coefOffset && 1 == left2coefOffset.type) 
                            return ret;
                        
                        if (! NumberUtils.stringIsNumber(left1Value)
                                || ! NumberUtils.stringIsNumber(left2Value)) 
                            return ret;
                        
                        double left1_value = Double.valueOf(NumberUtils.stringNumber(left1Value));
                        if (null != coefOffset) 
                            left1_value = (left1_value - coefOffset.offset)/coefOffset.coef;
                        
                        double left2_value = Double.valueOf(NumberUtils.stringNumber(left2Value));
                        if (null != left2coefOffset) 
                            left2_value = (left2_value - left2coefOffset.offset)/left2coefOffset.coef;
                        
                        double left_value = diffMarkValid2(leftExp,left1_value,left2_value);
                        
                        ret = diffMarkValid(left_value,midExp,right1,right2); //判断是否软报警条件(true/false)
                        return ret;
                        
                    }else{//L1_ID=L2_ID
                        //String lastData = redisService.getValueByDataId(vid, left1,jedis);
                        String lastData = "";
//                        lastData = CTFOUtils.getDefaultCTFOCacheTable().queryHash(vid, left1);
                        Map<String, String>last=lastCache.get(vid);
                        if(null !=last)
                            lastData = last.get(left1);
                        if(!ObjectUtils.isNullOrEmpty(lastData) ){ //上传的实时数据包含左1字段
                            
                            if ((left2Value.contains("|") && !lastData.contains("|"))
                                    ||(!left2Value.contains("|") && lastData.contains("|"))
                                    ||(left2Value.contains(":") && !lastData.contains(":"))
                                    ||(!left2Value.contains(":") && lastData.contains(":"))
                                    ||(left2Value.contains("_") && !lastData.contains("_"))
                                    ||(!left2Value.contains("_") && lastData.contains("_"))) {
                                return ret;
                            }
                            
                            if (left2Value.contains("|")) {
                                
                                String[] larr = lastData.split("\\|");
                                String[] arr = left2Value.split("\\|");
                                
                                if (arr.length != larr.length) {
                                    return ret;
                                }
                                
                                for(int i =0;i<arr.length;i++){
                                    String larri = larr[i];
                                    String arri = arr[i];
                                    if (! ObjectUtils.isNullOrEmpty(larri)
                                            && ! ObjectUtils.isNullOrEmpty(arri)) {
                                        
                                        String lv= new String(Base64.decode(new String(larri)),"GBK");
                                        String v= new String(Base64.decode(new String(arri)),"GBK");
                                        
                                        if (lv.contains(":") && v.contains(":")){
                                            String [] larr2m = lv.split(":");
                                            String [] arr2m = v.split(":");
                                            if (larr2m.length != arr2m.length) {
                                                return ret;
                                            }
                                            if (arr2m.length ==2 
                                                    && !ObjectUtils.isNullOrEmpty(larr2m[1])
                                                    && !ObjectUtils.isNullOrEmpty(arr2m[1])) {
                                                
                                                String[] larr2 = larr2m[1].split("_");
                                                String[] arr2 = arr2m[1].split("_");
                                                if (larr2.length == arr2.length) {
                                                    
                                                    for(int j=0;j<arr2.length;j++){
                                                        double left1_value = Double.parseDouble(NumberUtils.stringNumber(larr2[j]));
                                                        double left2_value = Double.parseDouble(NumberUtils.stringNumber(arr2[j]));
                                                        if (null != coefOffset) {
                                                            left1_value = (left1_value - coefOffset.offset)/coefOffset.coef;
                                                            left2_value = (left2_value - coefOffset.offset)/coefOffset.coef;
                                                        }
                                                        double left_value = diffMarkValid2(leftExp,left1_value,left2_value);
                                                        ret = diffMarkValid(left_value,midExp,right1,right2); //判断是否软报警条件(true/false)
                                                        if(ret==1) return ret;
                                                    }
                                                    
                                                }
                                            }
                                        }
                                    } else {
                                        return ret;
                                    }
                                }
                                
                                return ret;
                            }
                            double left1_value = Double.valueOf(NumberUtils.stringNumber(lastData));
                            double left2_value = Double.valueOf(NumberUtils.stringNumber(left2Value));
                            if (null != coefOffset) {
                                left1_value = (left1_value - coefOffset.offset)/coefOffset.coef;
                                left2_value = (left2_value - coefOffset.offset)/coefOffset.coef;
                            }
                            double left_value = diffMarkValid2(leftExp,left1_value,left2_value);
                            ret = diffMarkValid(left_value,midExp,right1,right2); //判断是否软报警条件(true/false)
                        }
                    }
                    
                }
                
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    private void sendAlarmMessage(int ret,String vid,EarlyWarn warn,Map<String,String> dataMap){
        if(null == warn)return;
        String filterId = warn.id;
        String alarmName = warn.name;
        int alarmLevel = warn.levels;
        String left1 = warn.left1DataItem; //左1数据项ID
        String left2 = warn.left2DataItem; //左2数据项ID
        double right1 = warn.right1Value;
        double right2 = warn.right2Value;
        String alarmGb = dataMap.get(DataKey._2920_ALARM_STATUS);
        alarmGb = NumberUtils.stringNumber(alarmGb);
        if (!"0".equals(alarmGb)) {
            int gbAlarm = Integer.parseInt(alarmGb);
            alarmLevel = Math.max(alarmLevel, gbAlarm);
        }
        String time = dataMap.get(SysDefine.TIME);
        long alarmUtc = getTime(time);
        alarmUtc = 0==alarmUtc?System.currentTimeMillis():alarmUtc;
        String vidFilterId = vid+"_"+filterId;
        List<String>list=filterMap.get(vid);
        if(ret == 1){
            //报警缓存包含vid，且vid对应的list含有此约束id，也就是此类型的报警，就说明上一条已报警
            
            if(!ObjectUtils.isNullOrEmpty(list) && list.contains(filterId)){
                //上条报警，本条也报警，说明是【报警进行中】，发送报警进行中报文
//                String alarmId = alarmMap.get(vid+"#"+filterId);
//                StringBuilder alarmKafka=new StringBuilder("VEHICLE_ID:");
//                alarmKafka.append(vid).append(",ALARM_ID:").append(alarmId).append(",STATUS:2,TIME:").append(time).append(",CONST_ID:").append(filterId);
//                
//                Map<String,String> sendMsg = new TreeMap<String,String>();
//                sendMsg.put(DataKey.VEHICLE_ID, vid);
//                sendMsg.put("ALARM_ID", alarmId);
//                sendMsg.put("STATUS", "2");
//                sendMsg.put("TIME", time);
//                sendMsg.put("CONST_ID", filterId);                
                //kafka存储
                //持续中的告警不通知
//                sendAlarmKafka(SysDefine.VEH_ALARM,vehAlarmTopic,vid, alarmKafka.toString());
//                alarmKafka=null;
                //发送到 故障判断处理节点继续
//                sendMsg.put("UTC_TIME", ""+alarmUtc);
//                sendToNext(SysDefine.FAULT_GROUP,vid, sendFault);
                //redis存储
                saveRedis(vid,"1",time);
                vid2AlarmInfo.put(vidFilterId, (alarmNum + 1) + "_0_" + alarmUtc);
                /*******cache********/
                Set<String> infoIds = vidAlarmIds.get(vid);
                if (null == infoIds) {
                    infoIds = new HashSet<String>();
                    vidAlarmIds.put(vid, infoIds);
                }
                infoIds.add(vidFilterId);
                /********cache*******/
            }else{
                //上条不报警，本条报警，说明是【开始报警】，发送开始报警报文
                //String alarmId = vid +"_" + GeneratorPK.instance().getPKString();
                String string=vid2AlarmInfo.get(vidFilterId);
                if(!ObjectUtils.isNullOrEmpty(string)){
                    String []infoArr= string.split("_",3);
                    if (infoArr.length >=3) {
                        int alarmNumThid = Integer.valueOf(infoArr[0]);
                        long alarmTime = Long.parseLong(infoArr[1]);
                        long lastAlarmUtc = Long.parseLong(infoArr[2]);
                        vid2AlarmInfo.put(vidFilterId, (alarmNumThid + 1) + "_" + alarmTime + "_" + lastAlarmUtc);
                        /*******cache********/
                        Set<String> infoIds = vidAlarmIds.get(vid);
                        if (null == infoIds) {
                            infoIds = new HashSet<String>();
                            vidAlarmIds.put(vid, infoIds);
                        }
                        if (!infoIds.contains(vidFilterId)) {
                            infoIds.add(vidFilterId);
                        }
                        /********cache*******/

                        //**根据数据项的预警配置进行判断，条件成立针对最后预警发生时间或者个数累计进行进行判定，若超过3分钟或连续累计超过10次的条件方认为成立*//*
                        if(alarmNumThid+1 >= alarmNum){
                            //|| alarmTime >= this.alarmTime
                            String alarmId = vid +"_" + time+"_"+filterId;
                            alarmMap.put(vid+"#"+filterId, alarmId);
                            List<String> l=filterMap.get(vid);
                            if(null==l){
                                l = new LinkedList<String>();
                            }
                            l.add(filterId);
                            if (! needListenAlarmSet.contains(vid)) {
                                needListenAlarmSet.add(vid);
                                needListenAlarms.offer(vid);
                            }
                            filterMap.put(vid, l);
//                            StringBuilder alarmStart=new StringBuilder("VEHICLE_ID:");
//                            alarmStart.append(vid).append(",ALARM_ID:").append(alarmId).append(",STATUS:1,TIME:").append(getTimeStr(lastAlarmUtc)).append(",CONST_ID:").append(filterId);
                            
                            Map<String,Object> sendMsg = new TreeMap<String,Object>();
                            sendMsg.put(DataKey.VEHICLE_ID, vid);
                            sendMsg.put("ALARM_ID", alarmId);
                            sendMsg.put("STATUS", 1);
                            sendMsg.put("TIME", getTimeStr(lastAlarmUtc));
                            sendMsg.put("CONST_ID", filterId);
                            sendMsg.put("ALARM_LEVEL", alarmLevel);
                            String alarmStart = JSON.toJSONString(sendMsg);
                            
                            sendMsg.put("ALARM_NAME", alarmName);
                            sendMsg.put("LEFT1", left1);
                            sendMsg.put("LEFT2", left2);
                            sendMsg.put("RIGHT1", right1);
                            sendMsg.put("RIGHT2", right2);
                            String alarmHbase = JSON.toJSONString(sendMsg);
                            //发送kafka提供数据库存储
                            sendAlarmKafka(SysDefine.VEH_ALARM,vehAlarmTopic,vid, alarmStart);
                            //hbase存储
                            sendAlarmKafka(SysDefine.VEH_ALARM_REALINFO_STORE,vehAlarmStoreTopic, vid, alarmHbase);
                            
                            sendMsg.put("UTC_TIME", lastAlarmUtc);
                            //发送到 故障判断处理节点继续
//                            sendMsg.put("COUNT",1234 );
//                            sendMsg.put("ALARM_VAL", "xxxxxx");
//                            sendMsg.put("UTC_TIME", ""+lastAlarmUtc);
                            //sendToNext(SysDefine.FAULT_GROUP,vid, sendMsg);
                            //redis存储
                            saveRedis(vid,"1",getTimeStr(lastAlarmUtc));
                            vid2AlarmInfo.put(vidFilterId, (alarmNumThid + 1) + "_" + alarmTime + "_" + alarmUtc);
                        }
                    }
                    
                }else {
                    vid2AlarmInfo.put(vidFilterId, "1_0_" + alarmUtc);
                    /*******cache********/
                    Set<String> infoIds = vidAlarmIds.get(vid);
                    if (null == infoIds) {
                        infoIds = new HashSet<String>();
                        vidAlarmIds.put(vid, infoIds);
                    }
                    if (!infoIds.contains(vidFilterId)) {
                        infoIds.add(vidFilterId);
                    }
                    /********cache*******/
                }

            }
            vid2AlarmEnd.remove(vidFilterId);
        }else if(ret ==2){
            if(!ObjectUtils.isNullOrEmpty(list) && list.contains(filterId)){
                String countTime = vid2AlarmEnd.get(vidFilterId);
                if(!ObjectUtils.isNullOrEmpty(countTime)){
                    
                    String[] ctArr = countTime.split("_");
                    vid2AlarmEnd.put(vidFilterId, Integer.valueOf(ctArr[0])+1 +"_"+ctArr[1]);
                    if(Integer.valueOf(ctArr[0]) == (alarmNum-1)){
                        //vid2alarmInfo.put(vid+"_"+filterId, "0_0_"+alarmUtc);
                        //上条报警，本条不报警，说明是【结束报警】，发送结束报警报文
                        String alarmId = alarmMap.get(vid+"#"+filterId);
                        //String alarmEnd = "VEHICLE_ID:"+vid+",ALARM_ID:"+alarmId+",STATUS:3,TIME:"+ctArr[1]+",CONST_ID:"+filterId;
                        
                        Map<String,Object> sendMsg = new TreeMap<String,Object>();
                        sendMsg.put(DataKey.VEHICLE_ID, vid);
                        sendMsg.put("ALARM_ID", alarmId);
                        sendMsg.put("STATUS", 3);
                        sendMsg.put("TIME", ctArr[1]);
                        sendMsg.put("CONST_ID", filterId);
                        String alarmEnd = JSON.toJSONString(sendMsg);
                        
                        sendMsg.put("ALARM_NAME", alarmName);
                        sendMsg.put("ALARM_LEVEL", alarmLevel);
                        sendMsg.put("LEFT1", left1);
                        sendMsg.put("LEFT2", left2);
                        sendMsg.put("RIGHT1", right1);
                        sendMsg.put("RIGHT2", right2);
                        String alarmHbase = JSON.toJSONString(sendMsg);
                        
                        //kafka存储
                        sendAlarmKafka(SysDefine.VEH_ALARM,vehAlarmTopic,vid, alarmEnd);
                        //hbase存储
                        sendAlarmKafka(SysDefine.VEH_ALARM_REALINFO_STORE,vehAlarmStoreTopic, vid, alarmHbase);
                        //发送到 故障判断处理节点继续
            //sendMsg.put("UTC_TIME", getTime(ctArr[1]));
                        //sendToNext(SysDefine.FAULT_GROUP,vid, sendMsg);
                        //redis存储
                        saveRedis(vid,"0",ctArr[1]);
                        alarmMap.remove(vid+"#"+filterId);
                        filterMap.get(vid).remove(filterId);
                        vid2AlarmEnd.remove(vidFilterId);
                    }
                }else{
                    vid2AlarmEnd.put(vidFilterId, "1_" + time);
                }
            }
            vid2AlarmInfo.remove(vidFilterId);
        }
    }

    /**
     * 存储更新redis
     * @param vid
     */
    private void saveRedis(String vid, String status,String time) {
        vid2Alarm.put(vid, status + "_" + System.currentTimeMillis() + "_" + time);
    }
  //synchronized
    private synchronized void sendAlarmKafka(String define,String topic,String vid, String message) {
        collector.emit(define, new Values(topic, vid, message));
    }
    
    private synchronized void sendToNext(String define,String vid, Object message) {
        collector.emit(define, new Values(vid, message));
    }

    private int diffMarkValid(double value, int mark, double right1, double right2) {
        int ret = 0;
        switch (mark) {
            case 1:
                if(value == right1){
                    ret = 1;
                }else{
                    ret = 2;
                }
                break;
            case 2:
                if(value < right1){
                    ret = 1;
                }else{
                    ret = 2;
                }
                break;
            case 3:
                if(value <= right1){
                    ret = 1;
                }else{
                    ret = 2;
                }
                break;
            case 4:
                if(value > right1){
                    ret = 1;
                }else{
                    ret = 2;
                }
                break;
            case 5:
                if(value >= right1){
                    ret = 1;
                }else{
                    ret = 2;
                }
                break;
            case 6:
                if(value > right1 && value < right2){
                    ret = 1;
                }else{
                    ret = 2;
                }
                break;
            case 7:
                if(value >= right1 && value < right2){
                    ret = 1;
                }else{
                    ret = 2;
                }
                break;
            case 8:
                if(value > right1 && value <= right2){
                    ret = 1;
                }else{
                    ret = 2;
                }
                break;
            case 9:
                if(value >= right1 && value <= right2){
                    ret = 1;
                }else{
                    ret = 2;
                }
                break;
            default:
                break;
        }

        return ret;

    }

    private double diffMarkValid2(int mark, double left1, double left2) {
        double ret =0;
        switch (mark) {
            case 1:
                ret = left1 + left2;
                break;
            case 2:
                ret = left1 - left2;
                break;
            case 3:
                ret = left1 * left2;
                break;
            case 4:
                if (0 == left2) 
                    break;
                
                ret = left1 / left2;
                break;
            default:
                break;
        }

        return ret;

    }
    private SimpleDateFormat getInDateFormat(){
        SimpleDateFormat format = formatlocal.get();
        if (null == format) {
            format = new SimpleDateFormat(DATA_FORMAT);
            formatlocal.set(format);
        }
        return formatlocal.get();
    }
    public long getTime(String gpsdate){
        long ret = 0;
        try {
            
            Date d = getInDateFormat().parse(gpsdate);
                ret = d.getTime();
            } catch (ParseException e) {
                e.printStackTrace();
        }

        return ret;
    }

    public String getTimeStr(long timeUtc){
        try {
            return getInDateFormat().format(new Date(timeUtc));
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    void removeByVid(String vid) {
        try {
            if (null != vid) {
                Set<String> idSet = vidAlarmIds.get(vid);
                if (null != idSet && idSet.size() > 0) {
                    for (String id : idSet) {
                        vid2AlarmInfo.remove(id);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    void timeOutOver(long offtime){
        try {
            
            if (needListenAlarms.size() >0) {
                List<String> needReListens =  new LinkedList<String>();
                
                String vid = needListenAlarms.poll();
                while (null != vid) {
                    needListenAlarmSet.remove(vid);
                    if (null != lastCache && lastCache.size() >0
                            && null != filterMap && filterMap.size()>0) {
                        long now = System.currentTimeMillis();
                        
                        if (filterMap.containsKey(vid)) {
                            
                            Map<String, String> dat = lastCache.get(vid);
                            if (null !=dat && dat.size()>0) {
                                if (dat.containsKey(SysDefine.ONLINEUTC)) {
                                    
                                    long timels = Long.parseLong(dat.get(SysDefine.ONLINEUTC));
                                    if (now - timels > offtime) {
                                        String vType = dat.get("VTYPE");
                                        if (!ObjectUtils.isNullOrEmpty(vid)
                                                && !ObjectUtils.isNullOrEmpty(vType) ) {
                                            
                                            List<EarlyWarn> warns = EarlyWarnsGetter.allWarnArrsByType(vType);
                                            if (!ObjectUtils.isNullOrEmpty(warns)) {
                                                
                                                sendOverAlarmMessage(vid);
                                            }
                                        }
                                        
                                    }else{
                                        needReListens.add(vid);
                                    }
                                    
                                }
                                
                            }
                        }
                        
                    }
                    
                    vid = needListenAlarms.poll();
                }
                
                if (needReListens.size() > 0) {
                    for (String key : needReListens) {
                        if (! needListenAlarmSet.contains(key)) {
                            needListenAlarmSet.add(key);
                            needListenAlarms.offer(key);
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}