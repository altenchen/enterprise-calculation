package ent.calc.storm.spout.kafka.scheme;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RegistScheme implements Scheme {

	private static final long serialVersionUID = 18700005L;
	private static Logger logger = LoggerFactory.getLogger(RegistScheme.class);

	@Override
	public List<Object> deserialize(ByteBuffer buffer) {
	    try {
            String string= StringScheme.deserializeString(buffer);
            return new Values(string);
        }
        catch (Exception exception) {
            exception.printStackTrace();
	        logger.error(exception.getMessage(), exception);
        }
        return new Values();
    }
	@Override
	public Fields getOutputFields() {
        return new Fields("msg");
    }
}
