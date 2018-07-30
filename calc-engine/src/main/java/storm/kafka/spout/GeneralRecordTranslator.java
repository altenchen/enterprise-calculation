package storm.kafka.spout;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.KafkaTuple;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.stream.FromGeneralToFilterStream;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: xzp
 * @date: 2018-08-11
 * @description: 实时消息转译器, Spout应该尽可能轻量级, 所以消息解码放到下游, 这里只是提取VID, 这样才能按VID分组消息.
 */
public final class GeneralRecordTranslator implements RecordTranslator<String, String> {

    @NotNull
    private static Logger logger = LoggerFactory.getLogger(GeneralRecordTranslator.class);

    @NotNull
    private static FromGeneralToFilterStream declare = FromGeneralToFilterStream.getInstance();

    /**
     * 提取VID
     * 消息结构：消息前缀 序列号 VIN码 命令标识 参数集
     */
    @NotNull
    private static final Pattern PICK_VID = Pattern.compile("^[^{]+\\{VID:([^,]+)");

    @Nullable
    @Override
    public List<Object> apply(@NotNull final ConsumerRecord<String, String> record) {
        final String value = record.value();

        Matcher matcher = PICK_VID.matcher(value);
        if (!matcher.find()) {
            logger.warn("无法取到VID的非法输入:[{}]", record.toString());
            return null;
        }

        final String vid = matcher.group(1);
        return new KafkaTuple(vid, value).routedTo(declare.getStreamId());
    }

    @NotNull
    @Override
    public Fields getFieldsFor(@NotNull final String streamId) {
        return declare.getFields();
    }

    @NotNull
    @Override
    public List<String> streams() {
        return Arrays.asList(declare.getStreamId());
    }
}
