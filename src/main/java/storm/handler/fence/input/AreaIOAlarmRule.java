package storm.handler.fence.input;

public class AreaIOAlarmRule extends RulePoJo implements CustomAlarmRule {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	RuleType type;
	
	public String ioType;//进入in，出out，进出 inout
	{
		setType(RuleType.RESULT.setResultType(Boolean.class));
	}
	@Override
	public RuleType getType() {

		return type;
	}
	
	void setType(RuleType type) {
		this.type = type;
	}


}
