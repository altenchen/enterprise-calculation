package storm.kafka.scheme;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.util.ObjectUtils;

public class DefaultScheme implements Scheme {
	private static final long serialVersionUID = 18700005L;
	private static Logger logger = LoggerFactory.getLogger(DefaultScheme.class);
	@Override
	public List<Object> deserialize(ByteBuffer buffer) {
		try {
			String string=ObjectUtils.deserialize(buffer);
			if(null != string)
				return new Values(string);
		} catch (Exception e) {
			e.printStackTrace();
			logger.info(e.getMessage());
		}
		return null;
    }
	@Override
	public Fields getOutputFields() {
        return new Fields("msg");
    }
}
