package storm.bolt.deal.cusmade;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import com.google.common.cache.Cache;

import storm.cache.SystemCache;
import storm.system.SysDefine;
import storm.util.CTFOUtils;
import storm.util.NumberUtils;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

@SuppressWarnings("all")
public class QuickCacheBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1700001L;
	private OutputCollector collector;
	private Map<String, Cache<String, Map<String,String>>>yearCache;
    public static ScheduledExecutorService service;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        initCache();
    }
    @Override
    public void execute(Tuple tuple) {
    	if(tuple.getSourceStreamId().equals(SysDefine.SUPPLY_GROUP)){
    		String vid = tuple.getString(0);
            Map<String, String> data = (TreeMap<String, String>) tuple.getValue(1);
            //supply
            handle(vid,data);
    	}
    	
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
    }

    private void handle(String vid,Map<String, String> data){
    	String time=data.get("2000");
        String mile=NumberUtils.stringNumber(data.get("2202"));
        String vin=data.get(SysDefine.VIN);
        if(StringUtils.isEmpty(time)
        		||StringUtils.isEmpty(mile)
        		||StringUtils.isEmpty(vin)) {
			return;
		}
        String year=time.substring(0, 4);
        long mileage = Long.parseLong(mile);
        long timeLong = Long.parseLong(time);
        
        Map<String,String> result=getCacheData(year,vid);
        if(null == result){
        	result=new TreeMap<String,String>();
        	result.put("vid", vid);
        	result.put("vin", vin);
        	result.put("minMileage", ""+mileage);
        	result.put("maxMileage", ""+mileage);
        	result.put("minMileageTime", time);
        	result.put("maxMileageTime", time);
        	result.put("minTime", time);
        	result.put("maxTime", time);
        	result.put("minTimeMileage", ""+mileage);
        	result.put("maxTimeMileage", ""+mileage);
        }else{
        	long minMileage=Long.valueOf(result.get("minMileage"));
            long maxMileage=Long.valueOf(result.get("maxMileage"));
            long minTime=Long.valueOf(result.get("minTime"));
            long maxTime=Long.valueOf(result.get("maxTime"));
            if (minMileage>mileage) {
            	result.put("minMileage", ""+mileage);
            	result.put("minMileageTime", time);
			}
            if(maxMileage<mileage){
            	result.put("maxMileage", ""+mileage);
            	result.put("maxMileageTime", time);
            }
            if(minTime>timeLong){
            	result.put("minTime", time);
            	result.put("minTimeMileage", ""+mileage);
            }
            if(maxTime<timeLong){
            	result.put("maxTime", time);
            	result.put("maxTimeMileage", ""+mileage);
            }
        }
        	
    	try {
			CTFOUtils.getCacheTable(year).addHash(vid, result);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
    private void initCache(){
    	yearCache=new TreeMap<String, Cache<String, Map<String,String>>>();
    	Calendar calendar =Calendar.getInstance();
    	calendar.setTime(new Date());
    	int year = calendar.get(Calendar.YEAR);
    	for(int y=year;y>year-3;y--){
    		yearCache.put(""+year, SystemCache.newDefaultCache());
    	}
    }
    
    private Cache<String, Map<String,String>> getCache(String name){
    	Cache<String, Map<String,String>> cache=yearCache.get(name);
    	if(null == cache){
    		cache=SystemCache.newDefaultCache();
    		yearCache.put(name, cache);
    	}
    	return cache;
    }
    
    private Map<String, String> getCacheData(final String year,final String vid){
    	Cache<String, Map<String,String>>cache=getCache(year);
    	Map<String,String> result=null;
    	try {
			result=cache.get(vid, new Callable<Map<String,String>>() {

				@Override
				public Map<String, String> call() throws Exception {
					Map<String, String> map=null;
					map=CTFOUtils.getCacheTable(year).queryHash(vid);
					if(null == map || map.size()==0) {
						map=null;
					}
					return map;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
    	return result;
    }
}
