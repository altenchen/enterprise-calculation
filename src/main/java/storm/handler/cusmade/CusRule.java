package storm.handler.cusmade;

public interface CusRule {

	enum CusRuleType{
		NOTNULL,
		VALID;//有效性，比如说超时，超速
	}
	String getItem();
	CusRuleType geType();
}
                                                  