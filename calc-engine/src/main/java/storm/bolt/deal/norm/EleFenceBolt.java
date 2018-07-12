package storm.bolt.deal.norm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import storm.dto.fence.Coordinate;
import storm.dto.fence.EleFence;
import storm.handler.fence.input.Rule;
import storm.handler.fence.output.Invoke;
import storm.handler.fence.output.Invoke.Result;
import storm.handler.fence.output.InvokeCtxMtd;
import storm.handler.fence.output.InvokeSglMtd;
import storm.system.DataKey;
import storm.system.SysDefine;
import storm.util.GsonUtils;
import storm.util.dbconn.Conn;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;

@SuppressWarnings("all")
public class EleFenceBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1700001L;
    private static final GsonUtils gson = GsonUtils.getInstance();
    private OutputCollector collector;
    private Map<String, EleFence>fenceCache;//key:fenceId,value:EleFence
    private Map<String, List<EleFence>>vidFenceCache;//key:vid,value:
    private List<EleFence> defaultfences;//默认围栏，对所有车子都有效
    private Invoke singleInvoke;
    private Invoke ctxInvoke;
    private Conn conn;
    private static String fenceAlarmTopic;
    private long lastExeTime;
    private long flushtime;
    Map<String, Boolean> vidInfence;
    Map<String, Boolean> vidOutfence;
    public static ScheduledExecutorService service;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        initFence();
        singleInvoke=new InvokeSglMtd();
        ctxInvoke=new InvokeCtxMtd();
        fenceAlarmTopic = stormConf.get("kafka.topic.fencealarm").toString();
        vidInfence = new HashMap<String, Boolean>();
        vidOutfence = new HashMap<String, Boolean>();
        try {
            conn = new Conn();
            vidFenceCache = conn.vidFences();
            if(null !=vidFenceCache && vidFenceCache.size() >0){

                System.out.println("vidFenceCache----------------->"+vidFenceCache.size());
                for (Map.Entry<String, List<EleFence>> entry : vidFenceCache.entrySet()) {
                    System.out.print("--------->"+entry.getKey()+":");
                    List<EleFence> fences = entry.getValue();
                    for (EleFence eleFen : fences) {

                        System.out.println(eleFen.toString());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        lastExeTime=System.currentTimeMillis();
        flushtime=1000*Long.parseLong(stormConf.get(SysDefine.DB_CACHE_FLUSH_TIME_SECOND).toString());
    }
    @Override
    public void execute(Tuple tuple) {
        try {
            long now = System.currentTimeMillis();
            if (now - lastExeTime >= flushtime){
                lastExeTime=now;
                vidFenceCache = conn.vidFences();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(tuple.getSourceStreamId().equals(SysDefine.FENCE_GROUP)){
            String vid = tuple.getString(0);
            Map<String, String> data = (TreeMap<String, String>) tuple.getValue(1);
            if (null == data.get(DataKey.VEHICLE_ID)) {
                data.put(DataKey.VEHICLE_ID, vid);
            }

            //fence
            List<Map<String, Object>>resuts = handle(data);
            if(null != resuts && resuts.size()>0) {
                for (Map<String, Object> map : resuts) {
                    if(null != map && map.size()>0){

                        String jsonString= gson.toJson(map);
                        //kafka存储
                        sendAlarmKafka(SysDefine.FENCE_ALARM,fenceAlarmTopic,vid, jsonString);
                    }
                }
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(SysDefine.FENCE_ALARM, new Fields("TOPIC", DataKey.VEHICLE_ID, "VALUE"));
    }
    
    void sendAlarmKafka(String define,String topic,String vid, String message) {
        collector.emit(define, new Values(topic, vid, message));
    }
    
    private void initFence(){
        defaultfences = new LinkedList<EleFence>();
        //init defaultfences
        fenceCache = new HashMap<String,EleFence>();
        //init fenceCache
        vidFenceCache = new HashMap<String,List<EleFence>>();
        //init vidFenceCache
    }
    
    private List<Map<String, Object>> handle(Map<String, String> data){

        if (!isNullOrEmpty(data)) {
            String vid = data.get(DataKey.VEHICLE_ID);
            List<EleFence> fences = new LinkedList<EleFence>();
            if (!isNullOrEmpty(defaultfences)) {
                fences.addAll(defaultfences);
            }
            if (!isNullOrEmpty(vidFenceCache) && !isNullOrEmpty(vidFenceCache.get(vid))) {
                fences.addAll(vidFenceCache.get(vid));
            }
            if (!isNullOrEmpty(fences)) {
                String time = data.get("2000");
                String lon = data.get("2502");//经度
                String lan = data.get("2503");//纬度
                if (isNullOrEmpty(lon) || isNullOrEmpty(lan)) {
                    //remove 在围栏的车辆信息
                    return null;
                }

                List<Map<String, Object>> results = new LinkedList<Map<String, Object>>();
                double lo=Double.valueOf(lon);
                double la=Double.valueOf(lan);
                Coordinate coord = new Coordinate(lo, la);
                long msgTime=0L;
                if (!isNullOrEmpty(time)) {
                    msgTime=Long.valueOf(time);
                    System.out.println("FENCE------------------msgTime---"+msgTime);
                    for (EleFence eleFence : fences) {
                        System.out.println("FENCE------------------EleFenceID---"+eleFence.id+eleFence.toString());
                        if (!"1".equals(eleFence.status)) {
                            continue;
                        }
                        boolean nowIsalive = eleFence.nowIsalive();
                        if (!nowIsalive) {
                            continue;
                        }
                        boolean intimes = eleFence.stringTimeIntimes(time);
                        boolean incoord = eleFence.coordisIn(coord);

                        if (!intimes) {
                            continue;
                        }

                        Map<String, Object> judge = new TreeMap<String, Object>();
                        judge.put("NOTIFYTYPE", "FENCE_ALARM");
                        judge.put(DataKey.VEHICLE_ID, vid);
                        judge.put("FENCEID", eleFence.id);
                        judge.put("LOCATION", lon+","+lan);
                        judge.put("TIME", msgTime);

                        if (incoord && intimes) {
                            judge.put("FLAG", 1);
                            if(!isNullOrEmpty(eleFence.rules)){
                                Map<String, Object> rst = new TreeMap<String, Object>();
                                List<Object>codes=new LinkedList<Object>();
                                for (Rule rule : eleFence.rules) {
                                    Result r=singleInvoke.exe(data, rule);
                                    if (!r.equals(Result.VOID)) {
                                        Object rstval = r.getResultValue();
                                        if (null != rstval) {

                                            Map<String, Object> re=(Map<String, Object>)rstval;
                                            if (null !=re && re.size()>0) {
                                                rst.putAll(re);
                                                Object code = re.get(SysDefine.CODE);
                                                if (null !=code) {
                                                    codes.add(code);
                                                }
                                            }
                                        }
                                    }
                                    //因为需要缓存数据，所以还是需要过滤一下，有的规则没有的话就不用缓存，调用此方法
                                    Result r2=ctxInvoke.exe(data, rule);
                                    if (!r2.equals(Result.VOID)) {
                                        Object rstval = r2.getResultValue();
                                        if (null != rstval) {
                                            Map<String, Object> re=(Map<String, Object>)rstval;
                                            if (null !=re && re.size()>0) {
                                                rst.putAll(re);
                                                Object code = re.get(SysDefine.CODE);
                                                if (null !=code) {
                                                    codes.add(code);
                                                }
                                            }
                                        }
                                    }
                                }
                                if (rst.size()>0) {
                                    rst.put("alarmLocation", lon+","+lan);
                                    rst.put(SysDefine.CODE, codes);
                                    judge.put("DATAOTHER", rst);
                                }
                            }
                            if (vidInfence.containsKey(vid)) {
                                vidInfence.put(vid, true);
                            } else {
                                vidInfence.put(vid, false);
                            }

                            boolean ongoing = vidInfence.get(vid);
                            if (ongoing) {
                                judge.put("ONGOING", 1);
                            } else {
                                judge.put("ONGOING", 0);
                            }
                            vidOutfence.remove(vid);
                        }
                        if (!incoord && intimes) {
                            vidInfence.remove(vid);
                            judge.put("FLAG", 0);
                            if (vidOutfence.containsKey(vid)) {
                                vidOutfence.put(vid, true);
                            } else {
                                vidOutfence.put(vid, false);
                            }

                            boolean ongoing = vidOutfence.get(vid);
                            if (ongoing) {
                                judge.put("ONGOING", 1);
                            } else {
                                judge.put("ONGOING", 0);
                            }
                        }

                        results.add(judge);
                    }
                    return results;
                }
            }
        }
        return null;
    }
    boolean isNullOrEmpty(Collection collection){
        if (null == collection || collection.size()==0) {
            return true;
        }
        return false;
    }
    boolean isNullOrEmpty(Map map){
        if(map == null || map.size()==0) {
            return true;
        }
        return false;
    }
    boolean isNullOrEmpty(String string){
        if(null == string || "".equals(string)) {
            return true;
        }
        return "".equals(string.trim());
    }
    
    /*
     int pnpoly(int nvert, float *vertx, float *verty, float testx, float testy)
{
  int i, j, c = 0;
  for (i = 0, j = nvert-1; i < nvert; j = i++) {
    if ( ((verty[i]>testy) != (verty[j]>testy)) &&
     (testx < (vertx[j]-vertx[i]) * (testy-verty[i]) / (verty[j]-verty[i]) + vertx[i]) )
       c = !c;
  }
  return c;
}
/**
     * 判断 点是否在多边形的内部
     * @param point
     * @param polygon
     * @return
     */
    /*
    boolean isInPolygonWithJdkGeneralPath(List<Coordinate> coords, Coordinate coord) {

        java.awt.geom.GeneralPath p = new java.awt.geom.GeneralPath();
        Point2D.Double point = new Point2D.Double(coord.x, coord.y);
        Coordinate first = coords.get(0);
        p.moveTo(first.x, first.y);
        coords.remove(0);
        for (Coordinate coo : coords) {
            p.lineTo(coo.x, coo.y);
        }

        p.lineTo(first.x, first.y);
        p.closePath();
        return p.contains(point);
    }
    
    /**
     * 判断 点是否在多边形的内部
     * @param point
     * @param polygon
     * @return
     */
    /*
    boolean checkWithJdkPolygon(Point2D.Double point, List<Point2D.Double> polygon) {  
        java.awt.Polygon p = new Polygon();  
        // java.awt.geom.GeneralPath  
        final int times = 1000;  
        for (Point2D.Double d : polygon) {  
            int x = (int) d.x * times;  
            int y = (int) d.y * times;  
            p.addPoint(x, y);  
        }  
        int x = (int) point.x * times;  
        int y = (int) point.y * times;  
        return p.contains(x, y);  
    } 
    
     */
}
