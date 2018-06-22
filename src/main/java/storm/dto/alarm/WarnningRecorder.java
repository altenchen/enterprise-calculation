package storm.dto.alarm;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;

public class WarnningRecorder {

	Map<String, Map<String,WarnRecord>>carRecords;
	{
		carRecords =  new java.util.concurrent.ConcurrentHashMap<String, Map<String,WarnRecord>>();
	}
	
	public void addRecords(String vid,Map<String,WarnRecord>records) {
        if (!StringUtils.isEmpty(vid)
				&& null != records) {
			
			carRecords.put(vid, records);
		}
	}
	
	public Map<String, WarnRecord> getRecords(String vid) {
        if (!StringUtils.isEmpty(vid)) {
			return carRecords.get(vid);
		}
		return null;
	}
	
	public void putRecord(String vid,WarnRecord record){
        if (StringUtils.isEmpty(vid)
				|| null == record
				|| StringUtils.isEmpty(record.warnId)) {
			return ;
		}
		Map<String, WarnRecord> records = getRecords(vid);
		if (null == records) {
			records = new TreeMap<String,WarnRecord>();
			addRecords(vid, records);
		}
		String warnId = record.warnId;
		records.put(warnId, record);
	}
	
	public WarnRecord getWarnRecord(String vid,WarnRecord record){
		if (null == record) {
			return null;
		}
		return getWarnRecord(vid,record.warnId);
	}
	
	public WarnRecord getWarnRecord(String vid,String warnId){
        if (StringUtils.isEmpty(vid)
				|| StringUtils.isEmpty(warnId)) {
			return null;
		}
		Map<String, WarnRecord> records = getRecords(vid);
		if (null != records)
			return records.get(warnId);
		return null;
	}
	
}
