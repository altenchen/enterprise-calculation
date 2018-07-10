package storm.stream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.system.DataKey;

/**
 * @author: xzp
 * @date: 2018-06-06
 * @description:
 */
public final class CUS_NOTICE_GROUP {

    public static final String streamId = "cusnoticeGroup";

    public static final Fields fields = new Fields(DataKey.VEHICLE_ID, "DATA");

    public static CUS_NOTICE_GROUP prepareOnce(OutputCollector collector) {
        return new CUS_NOTICE_GROUP(collector);
    }

    public static void declareStreamOnce(OutputFieldsDeclarer declarer) {
        declarer.declareStream(streamId, fields);
    }


    private final OutputCollector _collector;
    private CUS_NOTICE_GROUP(OutputCollector collector) {
        this._collector = collector;
    }

    public void emit(String vid, Object data) {
        _collector.emit(streamId, new Values(vid, data));
    }
}
