package storm.util;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.ctfo.datacenter.cache.handle.CTFOCacheDB;
import com.ctfo.datacenter.cache.handle.CTFOCacheTable;
import com.ctfo.datacenter.cache.handle.CTFODBManager;
import com.ctfo.datacenter.cache.handle.DataCenter;
import ent.calc.util.ConfigUtils;

public class CTFONoOtherUtils {
    private static final ConfigUtils configUtils = ConfigUtils.getInstance();
	private static CTFODBManager ctfoDBManager;
    private static CTFOCacheDB ctfoCacheDB ;
    private static CTFOCacheTable ctfoCacheTable;
    
    static{
    	initCTFO(configUtils.sysDefine);
    }
    private static void initCTFO(Properties conf){
    	 try{
    		 	System.out.println("------ CTFONoOtherUtils init start...");
    		 	String string = conf.getProperty("ctfo.cacheHost") + ":" + conf.getProperty("ctfo.cachePort")+","+conf.getProperty("redis.timeOut");
    		 	System.out.println(string);
    	    	ctfoDBManager = DataCenter.newCTFOInstance("cache", conf.getProperty("ctfo.cacheHost") + ":" + conf.getProperty("ctfo.cachePort"),100);
    	    	ctfoCacheDB = ctfoDBManager.openCacheDB( conf.getProperty("ctfo.cacheDB"));
    	        ctfoCacheTable = ctfoCacheDB.getTable( conf.getProperty("ctfo.cacheTable"));
    	       
    	        System.out.println("------ CTFONoOtherUtils init end...");
    	    } catch (Exception e) {
    	    	System.out.println("ctfoCacheTable初始化异常");
    	    }
    }
    
    private static void reconnectionDefaultRedis(Properties conf) {
        while(true){
            try {
                ctfoCacheTable = ctfoCacheDB.getTable(conf.getProperty("ctfo.cacheTable"));
                if(ctfoCacheTable != null){
                    break;
                }
            } catch (Exception e) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }
    public static final CTFOCacheTable getDefaultCTFOCacheTable(){
    	if (null == ctfoCacheTable) {
            reconnectionDefaultRedis(configUtils.sysDefine);
        }
    	return ctfoCacheTable;
    }
    
    public static void main(String[] args) {
		System.out.println("...Main...");
	}
}
