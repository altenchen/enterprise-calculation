package storm.dto.alarm;

import java.util.Map;
import java.util.TreeMap;

import storm.util.ObjectUtils;

public class WarnningRecorder {

	Map<String, Map<String,WarnRecord>>carRecords;
	{
		carRecords =  new java.util.concurrent.ConcurrentHashMap<String, Map<String,WarnRecord>>();
	}
	
	public void addRecords(String vid,Map<String,WarnRecord>records) {
		if (! ObjectUtils.isNullOrEmpty(vid)
				&& null != records) {
			
			carRecords.put(vid, records);
		}
	}
	
	public Map<String, WarnRecord> getRecords(String vid) {
		if (! ObjectUtils.isNullOrEmpty(vid)) {
			return carRecords.get(vid);
		}
		return null;
	}
	
	public void putRecord(String vid,WarnRecord record){
		if (ObjectUtils.isNullOrEmpty(vid)
				|| null == record
				|| ObjectUtils.isNullOrEmpty(record.warnId)) {
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
		if (ObjectUtils.isNullOrEmpty(vid)
				|| ObjectUtils.isNullOrEmpty(warnId)) {
			return null;
		}
		Map<String, WarnRecord> records = getRecords(vid);
		if (null != records)
			return records.get(warnId);
		return null;
	}
	
}
