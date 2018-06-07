package storm.kafka.scheme;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.system.DataKey;
import storm.util.ObjectUtils;

/**
 * 实时信息方案
 * 在消息中提取出VID, 然后将VID和消息本身作为一个元组(VID, 消息)发射
 */
public class RealinfoScheme implements Scheme {

	private static final long serialVersionUID = 18700005L;
	static Logger logger = LoggerFactory.getLogger(RealinfoScheme.class);

	@Override
	public List<Object> deserialize(ByteBuffer buffer) {
        try {
        	String msg = ObjectUtils.deserialize(buffer);
        	if(null == msg) {
        	    return null;
            }
            return generateValues(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private static final Values generateValues(@NotNull String msg) {
        // 消息结构：消息前缀 序列号 VIN码 命令标识 参数集
        String[] data = msg.split(" ");
        if (data.length == 5){
            // 参数集移除括号和反斜杠
            String val = data[4].replaceAll("[\\pC{}]", "");
            int index = val.indexOf(",VTYPE");
            if (index > 0) {
                String header = val.substring(0, index);
                String[] VID = header.split(":", 2);
                if (VID.length == 2){
                    // 转换后格式: (VEHICLE_ID, msg)
                    return new Values(new String(VID[1]), msg);
                }
            }
        }
        return null;
    }

	@Override
	public Fields getOutputFields() {
		
		return new Fields(DataKey.VEHICLE_ID, "msg");
	}

	public static void main(String[] args) {
	    String[] messages = new String[]{
	        "SUBMIT 0 LKJTBKBY3HF012267 REALTIME {VID:5e87d889-f029-4767-a9bc-3fdfecdb5a08,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,2000:20180604224708,2501:0,2502:120243408,2503:31626550,9999:20180604224711}",
            "SUBMIT 0 LKJTBKBY3HF012267 HISTORY {VID:5e87d889-f029-4767-a9bc-3fdfecdb5a08,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,2000:20180604224657,2501:0,2502:120243408,2503:31626502,9999:20180604224701}",
            "SUBMIT 0 LKJTBKBY9HF012256 LOGIN {VID:074dcc9a-ee6f-4160-8ead-c310a3251856,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,RESULT:0,1025:20180605160704,1020:1,1021:ODk4NjAyQjUyNjE2MzAwNDM2ODU=,1022:1,1023:0,9999:20180605160705,1024:,PLATID:07e3aac0-87d3-4647-a06c-f409a1b060c3}",
            "SUBMIT 0 LKJTBKBY3HF012219 LINKSTATUS {VID:cc97b5df-ce83-49dc-8b31-a3898078d32a,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,TIME:20180605112409,TYPE:3}",
            "ERROR FORMAT",
            ""
	    };

        for (String message : messages) {
            Values values = RealinfoScheme.generateValues(message);
            if(values != null) {
                System.out.println(values.toString());
            }
        }
    }
}
