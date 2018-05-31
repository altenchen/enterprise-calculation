package storm.util;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctfo.datacenter.cache.exception.DataCenterException;
import com.ctfo.datacenter.cache.handle.CTFOCacheDB;
import com.ctfo.datacenter.cache.handle.CTFOCacheTable;
import com.ctfo.datacenter.cache.handle.CTFODBManager;
import com.ctfo.datacenter.cache.handle.DataCenter;

public class CTFOUtils implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 193000000001L;
	private static Logger logger = LoggerFactory.getLogger(CTFOUtils.class);
	private static CTFODBManager ctfoDBManager;
    private static CTFOCacheDB ctfoCacheDB ;
    private static CTFOCacheTable ctfoCacheTable;
    
    private static Map<String, CTFOCacheDB> dbMap;
    private static Map<String, CTFOCacheTable> tableMap;
    
    static {
    	try {
			initCTFO(ConfigUtils.sysDefine);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    private static void initMaps(){
    	try {
			dbMap=new TreeMap<String, CTFOCacheDB>();
			tableMap=new TreeMap<String, CTFOCacheTable>();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    private synchronized static void initCTFO(Properties conf){
    	 try{
    		 	System.out.println("------ CTFOUtils init start...");
    		 	try {
    		 		String addr = conf.getProperty("ctfo.cacheHost") + ":" + conf.getProperty("ctfo.cachePort");
    		 		System.out.println("------ CTFO addr..."+addr);
    		 		ctfoDBManager = DataCenter.newCTFOInstance("cache", addr);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(e);
				}
    	    	if (null != ctfoDBManager) {
					
    	    		ctfoCacheDB = ctfoDBManager.openCacheDB( conf.getProperty("ctfo.cacheDB"));
    	    		ctfoCacheTable = ctfoCacheDB.getTable( conf.getProperty("ctfo.cacheTable"));
    	    		
    	    		initMaps();
    	    		initDBTables();
    	    		System.out.println("------ CTFOUtils init end...");
    	    		logger.info("初始化 CTFO 成功...");
				} else {
					System.out.println("------ CTFOUtils init error...");
    	    		logger.info("初始化 CTFO 失败...");
				}
    	    } catch (Exception e) {
    	    	logger.warn("ctfoCacheTable初始化异常" + e);
    	    	System.out.println("ctfoCacheTable初始化异常");
    	    	reconnectionDefaultRedis(conf);
    	    }
    }
    
    private static void reconnectionDefaultRedis(Properties conf) {
    	int retry = 0;
        while(true){
            try {
            	retry++;
            	if(null != ctfoCacheDB)
            		ctfoCacheTable = ctfoCacheDB.getTable(conf.getProperty("ctfo.cacheTable"));
            	else {
            		try {
        		 		String addr = conf.getProperty("ctfo.cacheHost") + ":" + conf.getProperty("ctfo.cachePort");
        		 		ctfoDBManager = DataCenter.newCTFOInstance("cache", addr);
    				} catch (Exception e) {
    					e.printStackTrace();
    					System.out.println(e);
    				}
        	    	if (null != ctfoDBManager) {
    					
        	    		ctfoCacheDB = ctfoDBManager.openCacheDB( conf.getProperty("ctfo.cacheDB"));
        	    		ctfoCacheTable = ctfoCacheDB.getTable( conf.getProperty("ctfo.cacheTable"));
        	    		System.out.println("------ CTFOUtils relink success...");
        	    		logger.info("重连 CTFO 成功...");
    				}
        	    	if(null != ctfoCacheDB)
                		ctfoCacheTable = ctfoCacheDB.getTable(conf.getProperty("ctfo.cacheTable"));
        	    	
				}
                if(ctfoCacheTable != null){
                	logger.warn("----------redis重连成功！");
                    break;
                }
                if (30 < retry) {
                	try {
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
				}
                if (50 < retry) {
                	System.out.println("------ CTFOUtils relink fault...");
                	logger.warn("----------Ctfo redis重连失败！");
                    break;
				}
                logger.info("----------正在进行redis重连....");
            } catch (DataCenterException e) {
            	logger.warn("----------redis重连失败,3s后继续重连....");
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }
    public static final CTFOCacheTable getDefaultCTFOCacheTable(){
    	try {
			if (null == ctfoCacheTable) 
				reconnectionDefaultRedis(ConfigUtils.sysDefine);
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return ctfoCacheTable;
    }
    
    private static void initDBTables(){
    	try {
			Calendar calendar =Calendar.getInstance();
			calendar.setTime(new Date());
			int year = calendar.get(Calendar.YEAR);
			String table=ConfigUtils.sysDefine.getProperty("ctfo.supplyTable");
			for(int y=year;y>year-3;y--){
				initDBTable(ctfoDBManager,y+"",table);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    private static void initDBTable(CTFODBManager manager,String db,String table){
		try {
			CTFOCacheDB cacheDB = manager.openCacheDB(db);
			CTFOCacheTable cacheTable = cacheDB.getTable(table);
			dbMap.put(db, cacheDB);
			tableMap.put(db, cacheTable);
		} catch (DataCenterException e) {
			e.printStackTrace();
		}
        
    }
    private static CTFOCacheDB getDB(String name){
    	CTFOCacheDB cacheDB=dbMap.get(name);
    	try {
			if (null == cacheDB) {
				cacheDB = ctfoDBManager.openCacheDB(name);
				dbMap.put(name, cacheDB);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.warn("----------redis重连失败,3s后继续重连....");
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
		}
    	return cacheDB;
    }
    private static void reconnection(Properties conf,String name) {
        while(true){
            try {
            	CTFOCacheDB cacheDB=getDB(name);
            	CTFOCacheTable cacheTable = cacheDB.getTable( conf.getProperty("ctfo.supplyTable"));
                if(cacheTable != null){
                	logger.warn("----------supply redis重连成功！");
                	tableMap.put(name, cacheTable);
                    break;
                }
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                logger.info("----------正在进行supply redis重连....");
            } catch (Exception e) {
            	logger.warn("----------redis重连失败,3s后继续重连....");
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }
    public static final CTFOCacheTable getCacheTable(String name){
    	CTFOCacheTable cacheTable=null;
		try {
			cacheTable = tableMap.get(name);
			if (null == cacheTable) {
				reconnection(ConfigUtils.sysDefine,name);
				cacheTable =tableMap.get(name);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    		
    	return cacheTable;
    }
}
