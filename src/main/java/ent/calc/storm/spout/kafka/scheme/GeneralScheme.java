package ent.calc.storm.spout.kafka.scheme;

import com.google.common.collect.ImmutableList;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.system.DataKey;
import storm.util.ParamsRedisUtil;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 实时信息方案
 * 在消息中提取出VID, 然后将VID和消息本身作为一个元组(VID, 消息)发射
 * @author xzp
 */
public final class GeneralScheme implements Scheme {

	private static final long serialVersionUID = 18700005L;
	private static Logger logger = LoggerFactory.getLogger(GeneralScheme.class);
	private static final ParamsRedisUtil paramsRedisUtil = ParamsRedisUtil.getInstance();

    @SuppressWarnings("Duplicates")
	@Override
	public List<Object> deserialize(ByteBuffer buffer) {
	    try {
            String message = StringScheme.deserializeString(buffer);
            final Values values = generateValues(message);
            if(!values.isEmpty()) {
                final String vid = (String) values.get(0);
                logger.debug("收到VID[" + vid + "]的实时数据.");
                if(paramsRedisUtil.isTraceVehicleId(vid)) {
                    logger.info("收到VID[" + vid + "]的实时数据.");
                }
                return values;
            }
        }
        catch (Exception exception) {
            exception.printStackTrace();
            logger.error(exception.getMessage(), exception);
        }
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(DataKey.VEHICLE_ID, "msg");
    }

    /**
     * 提取VID
     * 消息结构：消息前缀 序列号 VIN码 命令标识 参数集
     */
    @NotNull
    private static final Pattern PICK_VID = Pattern.compile("^[^{]+\\{VID:([-0-9A-Za-z]+)");

    @Nullable
    private static Values generateValues(@NotNull String message) {
        Matcher matcher = PICK_VID.matcher(message);
        if(matcher.find()) {
            String vid = matcher.group(1);
            return new Values(vid, message);
        }
        return new Values();
    }

	public static void main(String[] args) {
        ImmutableList
            .of(
                new Values("5e87d889-f029-4767-a9bc-3fdfecdb5a08", "SUBMIT 0 LKJTBKBY3HF012267 REALTIME {VID:5e87d889-f029-4767-a9bc-3fdfecdb5a08,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,2000:20180604224708,2501:0,2502:120243408,2503:31626550,9999:20180604224711}"),
                new Values("5e87d889-f029-4767-a9bc-3fdfecdb5a08", "SUBMIT 0 LKJTBKBY3HF012267 HISTORY {VID:5e87d889-f029-4767-a9bc-3fdfecdb5a08,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,2000:20180604224657,2501:0,2502:120243408,2503:31626502,9999:20180604224701}"),
                new Values("074dcc9a-ee6f-4160-8ead-c310a3251856", "SUBMIT 0 LKJTBKBY9HF012256 LOGIN {VID:074dcc9a-ee6f-4160-8ead-c310a3251856,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,RESULT:0,1025:20180605160704,1020:1,1021:ODk4NjAyQjUyNjE2MzAwNDM2ODU=,1022:1,1023:0,9999:20180605160705,1024:,PLATID:07e3aac0-87d3-4647-a06c-f409a1b060c3}"),
                new Values("cc97b5df-ce83-49dc-8b31-a3898078d32a", "SUBMIT 0 LKJTBKBY3HF012219 LINKSTATUS {VID:cc97b5df-ce83-49dc-8b31-a3898078d32a,VTYPE:ff8080816252c33a016260d31c1a04b0,CTYPE:2_1_1,TIME:20180605112409,TYPE:3}"),
                new Values("", "ERROR FORMAT"),
                new Values("", "")
            )
            .stream()
            .map(tuple -> new Values(tuple.get(0), GeneralScheme.generateValues((String)tuple.get(1))))
            .forEach(tuple -> {
                final String expect = (String)tuple.get(0);
                final Values values = (Values)tuple.get(1);
                if("".equals(expect)) {
                    assert values.isEmpty() : "非法输入未忽略";
                } else {
                    final String actual = (String)values.get(0);
                    assert expect.equals(actual) : "提取的VID与预期不符";
                    System.out.println(actual);
                }
            });
    }
}
