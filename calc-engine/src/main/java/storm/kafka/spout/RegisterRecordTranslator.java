package storm.kafka.spout;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.stream.RegisterStream;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: xzp
 * @date: 2018-08-11
 * @description:
 */
public final class RegisterRecordTranslator implements RecordTranslator<String, String> {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(RegisterRecordTranslator.class);

    // region RegisterStream

    @NotNull
    private final RegisterStream.Sender registerStreamSender;

    // endregion RegisterStream

    /**
     * 提取VID
     * 消息结构：消息前缀 序列号 VIN码 命令标识 参数集
     */
    @NotNull
    private static final Pattern PICK_VID = Pattern.compile("VID:([^:,]+)");

    public RegisterRecordTranslator(
        @NotNull final RegisterStream.Sender registerStreamSender) {

        this.registerStreamSender = registerStreamSender;
    }

    @Nullable
    @Override
    public List<Object> apply(
        @NotNull final ConsumerRecord<String, String> record) {

        final String value = record.value();

        final Matcher matcher = PICK_VID.matcher(value);
        if (!matcher.find()) {
            logger.warn("无法取到VID的非法输入:[{}]", record.toString());
            return null;
        }

        final String vid = matcher.group(1);
        return registerStreamSender.emit(vid, value);
    }

    @NotNull
    @Override
    public Fields getFieldsFor(
        @NotNull final String streamId) {

        return registerStreamSender.getFields();
    }

    @NotNull
    @Override
    public List<String> streams() {

        return Arrays.asList(
            registerStreamSender.getStreamId());
    }
}
