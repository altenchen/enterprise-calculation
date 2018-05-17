package storm.handler.cusmade;

import java.util.List;
import java.util.Map;

public interface OnOffInfoNotice extends InfoNotice {
	List<Map<String, Object>> fulldoseNotice(String type,int status,long now,long timeout);//status:0全量数据，status:1活跃数据，status:2其他定义
	Map<String, Object> genotice(Map<String, String> dat,long now,long timeout);
	public void onoffCheck(String type, int status, long now, long timeout);
}
