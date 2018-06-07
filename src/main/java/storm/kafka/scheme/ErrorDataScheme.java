package storm.kafka.scheme;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import storm.protocol.CommandType;
import storm.system.DataKey;
import storm.util.ObjectUtils;

public class ErrorDataScheme implements Scheme {
	private static final long serialVersionUID = 134009L;
	@Override
	public List<Object> deserialize(ByteBuffer buffer) {
        try {
        	String msg = ObjectUtils.deserialize(buffer);
        	if(null == msg)return null;
            String[] data = msg.split(" ");

            if (data.length==5 && data[3].equals(CommandType.SUBMIT_PACKET)) {
                Map<String, String> map = new TreeMap<String, String>();
                String val = data[4].replaceAll("[\\pC{}]", "");
                String[] ERROR = val.split(",");
                for (String str : ERROR) {
                    String[] kv = str.split(":", 2);
                    if (kv.length == 2) {
                        map.put(new String(kv[0]), new String(kv[1]));
                    }
                }

                if (map.get(DataKey.VEHICLE_ID)!=null && map.get("4")!=null && map.get("4").equals("1")) {
                    return new Values(new String(map.get(DataKey.VEHICLE_ID)), map, 1);
                }
                ERROR=null;
                val=null;
                map=null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
	@Override
	public Fields getOutputFields() {
        return new Fields(DataKey.VEHICLE_ID, "msg", "flag");
    }
}
