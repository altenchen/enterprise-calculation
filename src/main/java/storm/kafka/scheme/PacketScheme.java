package storm.kafka.scheme;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.protocol.SUBMIT_PACKET;
import storm.system.DataKey;

public final class PacketScheme implements Scheme {

	private static final long serialVersionUID = 134009L;
    private static Logger logger = LoggerFactory.getLogger(PacketScheme.class);

    @SuppressWarnings("Duplicates")
    @Override
	public List<Object> deserialize(ByteBuffer buffer) {
        try {
            String msg = StringScheme.deserializeString(buffer);
        	return generateValues(msg);
        } catch (Exception exception) {
            exception.printStackTrace();
            logger.error(exception.getMessage(), exception);
        }
        return new Values();
    }
	@Override
	public Fields getOutputFields() {
        return new Fields(DataKey.VEHICLE_ID, "msg", "flag");
    }

    // 消息结构：消息前缀 序列号 VIN码 命令标识 参数集
    @NotNull
    private static final Pattern pattern = Pattern.compile("^[^{]+PACKET \\{(VID:[^}]+)\\}$");

    @Nullable
    private static final Values generateValues(@NotNull String message) {
        Matcher matcher = pattern.matcher(message);
        if(matcher.find()) {
            String content = matcher.group(1);
            Map<String, String> map = new TreeMap<String, String>();
            for (String item : content.split(",")) {
                String[] pair = item.split(":", 2);
                if(pair.length == 2) {
                    map.put(pair[0], pair[1]);
                }
            }
            final String verifyState = map.get(SUBMIT_PACKET.VERIFY_STATE);
            if(SUBMIT_PACKET.isVerifyFailure(verifyState)) {
                String vid = map.get(DataKey.VEHICLE_ID);
                return new Values(vid, map, 1);
            }
        }

        return null;
    }

    public static void main(String[] args) {
        String[] messages = new String[]{
            "SUBMIT 0 LKJTBKBY6HF012229 PACKET {VID:168c55e1-84f2-457e-8628-f14d5a0dcf21,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,1:1,2:IyMB/kxLSlRCS0JZNkhGMDEyMjI5AQAeEgYEFS8nAAE4OTg2MDJCNTI2MTYzMDA0MzMwOQEAow==,3:20180604214739,4:1,5:20180604214739}",
            "SUBMIT 0 LKJTBKBY6HF012229 PACKET {VID:168c55e1-84f2-457e-8628-f14d5a0dcf21,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,1:2,2:IyMC/kxLSlRCS0JZNkhGMDEyMjI5AQAQEgYEFS8oBQAHKsY4AeKWHms=,3:20180604214740,4:1,5:20180604214740}",
            "SUBMIT 0 LKJTBKBY6HF012229 PACKET {VID:168c55e1-84f2-457e-8628-f14d5a0dcf21,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,1:3,2:IyMD/kxLSlRCS0JZNkhGMDEyMjI5AQAQEgYEFS8eBQAHKsY4AeKWHlw=,3:20180604214730,4:0,5:20180604214740}",
            "SUBMIT 0 LKJTBKBY3HF012267 REALTIME {VID:5e87d889-f029-4767-a9bc-3fdfecdb5a08,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,2000:20180604224708,2501:0,2502:120243408,2503:31626550,9999:20180604224711}",
            "SUBMIT 0 LKJTBKBY3HF012267 HISTORY {VID:5e87d889-f029-4767-a9bc-3fdfecdb5a08,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,2000:20180604224657,2501:0,2502:120243408,2503:31626502,9999:20180604224701}",
            "SUBMIT 0 LKJTBKBY9HF012256 LOGIN {VID:074dcc9a-ee6f-4160-8ead-c310a3251856,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,RESULT:0,1025:20180605160704,1020:1,1021:ODk4NjAyQjUyNjE2MzAwNDM2ODU=,1022:1,1023:0,9999:20180605160705,1024:,PLATID:07e3aac0-87d3-4647-a06c-f409a1b060c3}",
            "SUBMIT 0 LKJTBKBY3HF012219 LINKSTATUS {VID:cc97b5df-ce83-49dc-8b31-a3898078d32a,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,TIME:20180605112409,TYPE:3}",
            "ERROR FORMAT",
            ""
        };

        for (String message : messages) {
            Values values = PacketScheme.generateValues(message);
            if(values != null) {
                System.out.println(values.toString());
            }
        }
    }
}
