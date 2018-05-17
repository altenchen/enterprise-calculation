package storm.kafka.scheme;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.util.ObjectUtils;
import storm.system.SysDefine;

public class RealinfoScheme implements Scheme {
	private static final long serialVersionUID = 18700005L;
	static Logger logger = LoggerFactory.getLogger(RealinfoScheme.class);
	@Override
	public List<Object> deserialize(ByteBuffer buffer) {
        try {
        	String msg = ObjectUtils.deserialize(buffer);
        	if(null ==msg)return null;
            String[] data = msg.split(" ");
            if (data.length == 5){
                String val = data[4].replaceAll("[\\pC{}]", "");
                int index = val.indexOf(",VTYPE");
                if (index > 0) {
                    String header = val.substring(0, index);
                    String[] VID = header.split(":", 2);
                    if (VID.length == 2){
                        return new Values(new String(VID[1]), msg);
                    }
                }
            }
            data=null;
            msg=null;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
	@Override
	public Fields getOutputFields() {
		
		return new Fields(SysDefine.VID, "msg");
	}
}
