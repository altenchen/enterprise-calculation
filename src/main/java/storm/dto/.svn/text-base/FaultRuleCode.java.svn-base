package storm.dto;

import java.util.LinkedList;
import java.util.List;

public class FaultRuleCode {

	public String ruleId;
	public String itemType;
	public List<FaultCode> codes;
	{
		codes = new LinkedList<FaultCode>();
	}
	public FaultRuleCode(String ruleId,String itemType) {
		super();
		this.ruleId = ruleId;
		this.itemType = itemType;
	}
	public void addFaultCode(FaultCode code){
		if (null != code) {
			codes.add(code);
		}
	}
}
