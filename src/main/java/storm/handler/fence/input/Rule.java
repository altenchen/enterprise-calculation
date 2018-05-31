package storm.handler.fence.input;

public interface Rule extends java.io.Serializable{

	enum RuleType{
		VOID,//只是数据处理规则
		RESULT;//需要返回结果
		public Class<?> resultType;

		public Class<?> getResultType() {
			return resultType;
		}

		public RuleType setResultType(Class<?> resultType) {
			if (VOID == this) 
				this.resultType=null;
			else
				this.resultType = resultType;
			return this;
		}
		
	}
	
	String getCode();
	RuleType getType();
	
	String getName();
	
	String getRuleCondition();
	String getScriptEngine();
	
	Object clone() throws CloneNotSupportedException;
}
                                                  