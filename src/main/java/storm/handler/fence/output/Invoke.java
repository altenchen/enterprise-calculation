package storm.handler.fence.output;

import java.util.Map;

import storm.handler.fence.input.Rule;

public interface Invoke {

	enum Result{
		VOID,//只是数据处理规则
		VALUE;//需要返回结果
		private Object result;
		private Class<?> resultClass;

		public Object getResultValue() {
			return result;
		}
		
		public Object getResultClass() {
			return resultClass;
		}
		
		private void setResultClass(){
			if (null != this.result)
				resultClass=this.result.getClass();
			else
				resultClass=null;
		}

		public Result setResultValue(Object result) {
			if (VOID == this) 
				this.result=null;
			else
				this.result = result;
			setResultClass();
			return this;
		}
		
	}
	Result exe(Rule rule);
	Result exe(Map<String, String> dat,Rule rule);
	Result exe(Map<String, String> dat,Rule rule,Map<String, Object>result);
	void invoke(String function,String funcName,Object ... args);
}
                                                  