package storm.dto.fault;

public class FaultMessage extends MessageContent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 17000500000122L;

	public String faultRuleId;
	public String vid;
	public FaultMessage(long startTime, String faultRuleId) {
		super(startTime);
		this.faultRuleId = faultRuleId;
	}
	@Override
	public String toString() {
		return "FaultMessage [faultRuleId=" + faultRuleId + ", vid=" + vid + ", msgId=" + msgId + ", startTime="
				+ startTime + ", endTime=" + endTime + ", time=" + time + ", sustainTime=" + sustainTime
				+ ", triggerCounter=" + triggerCounter + ", risk=" + risk + ", status=" + status + "]";
	}
}
