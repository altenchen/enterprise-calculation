package storm.stream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import storm.constant.StreamFieldKey;
import storm.system.SysDefine;

import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-06-06
 * @description:
 */
public final class FromFilterToCarNoticeStream implements IStreamBase {

    @NotNull
    private static final Fields fields = new Fields(StreamFieldKey.VEHICLE_ID, StreamFieldKey.DATA);

    @NotNull
    private static final String componentId = SysDefine.CHECK_FILTER_BOLT_ID;

    @NotNull
    private static final String streamId = "cusnoticeGroup";

    @NotNull
    private static final FromFilterToCarNoticeStream INSTANCE = new FromFilterToCarNoticeStream();

    @Contract(pure = true)
    public static FromFilterToCarNoticeStream getInstance() {
        return INSTANCE;
    }

    private FromFilterToCarNoticeStream() {

    }

    @Contract(pure = true)
    @NotNull
    @Override
    public Fields getFields() {
        return fields;
    }


    @Contract(pure = true)
    @NotNull
    @Override
    public String getComponentId() {
        return componentId;
    }

    @Contract(pure = true)
    @NotNull
    @Override
    public String getStreamId() {
        return streamId;
    }

    @Contract("_ -> new")
    @Override
    public @NotNull FromFilterToCarNoticeStream.Emiter buildStreamEmiter(@NotNull final OutputCollector collector) {
        return new Emiter(IStreamBase.super.buildStreamEmiter(collector));
    }

    // region 上游使用

    public static final class Emiter implements IStreamEmiter {
        private final IStreamEmiter emiter;

        public Emiter(final IStreamEmiter emiter) {
            this.emiter = emiter;
        }

        @Override
        public @NotNull String getStreamId() {
            return emiter.getStreamId();
        }

        @Override
        public @NotNull OutputCollector getOutputCollector() {
            return emiter.getOutputCollector();
        }

        @Override
        public void emit(@NotNull final Values values) {
            emiter.emit(values);
        }

        public void emit(@NotNull final String vid, @NotNull Map<String, String> data) {
            emiter.emit(new Values(vid, data));
        }
    }

    // endregion 上游使用

    // region 下游使用

    /**
     * 获取车辆ID
     * @param tuple
     * @return
     */
    @NotNull
    public String getVid(@NotNull Tuple tuple) {
        return tuple.getStringByField(StreamFieldKey.VEHICLE_ID);
    }

    /**
     * 获取数据帧
     * @param tuple
     * @return
     */
    @NotNull
    public Map<String, String> getData(@NotNull Tuple tuple) {
        return (Map<String, String>)tuple.getValueByField(StreamFieldKey.DATA);
    }

    // endregion 下游使用
}
