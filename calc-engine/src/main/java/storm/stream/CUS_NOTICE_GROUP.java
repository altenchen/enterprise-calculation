package storm.stream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jetbrains.annotations.NotNull;
import storm.constant.StreamFieldKey;

import java.util.Map;

/**
 * @author: xzp
 * @date: 2018-06-06
 * @description:
 */
public final class CUS_NOTICE_GROUP {

    @NotNull
    public static final String streamId = "cusnoticeGroup";

    @NotNull
    public static final Fields fields = new Fields(StreamFieldKey.VEHICLE_ID, StreamFieldKey.DATA);

    @NotNull
    public static CUS_NOTICE_GROUP prepareOnce(@NotNull OutputCollector collector) {
        return new CUS_NOTICE_GROUP(collector);
    }

    public static void declareStreamOnce(@NotNull OutputFieldsDeclarer declarer) {
        declarer.declareStream(streamId, fields);
    }

    @NotNull
    public static String getVid(@NotNull Tuple tuple) {
        return tuple.getStringByField(StreamFieldKey.VEHICLE_ID);
    }

    @NotNull
    public static Map<String, String> getData(@NotNull Tuple tuple) {
        return (Map<String, String>)tuple.getValueByField(StreamFieldKey.DATA);
    }


    @NotNull
    private final OutputCollector _collector;
    private CUS_NOTICE_GROUP(@NotNull OutputCollector collector) {
        this._collector = collector;
    }

    public final void emit(@NotNull String vid, @NotNull Map<String, String> data) {
        _collector.emit(streamId, new Values(vid, data));
    }
}
