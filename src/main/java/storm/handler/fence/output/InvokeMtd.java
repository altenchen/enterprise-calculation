package storm.handler.fence.output;

import java.util.Map;

import storm.handler.fence.input.Rule;

abstract public class InvokeMtd implements Invoke {

	@Override
	public Result exe(Rule rule) {

		return Result.VOID;
	}

	@Override
	public void invoke(String function, String funcName, Object... args) {
	}

	@Override
	public Result exe(Map<String, String> data, Rule rule) {
		
		return Result.VOID;
	}
	
	@Override
	public Result exe(Map<String, String> data, Rule rule,Map<String, Object>result) {
		
		return Result.VOID;
	}

}
