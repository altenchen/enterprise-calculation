package storm.handler.ctx;

import java.util.Map;

public interface Recorder {

	void save(int db,String type,String id,Map<String, Object>ctx);
	void save(int db,String type,Map<String,Map<String, Object>>ctxs);
	
	void del(int db,String type,String ... ids);
	void rebootInit(int db, String type,Map<String, Map<String, Object>> initMap);
}
