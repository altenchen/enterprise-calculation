package storm.handler.fence.output;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class ScriptInvoke extends InvokeMtd implements Invoke {

	private ScriptEngine engine;
	{
		ScriptEngineManager manager=new ScriptEngineManager();
		engine=manager.getEngineByName("js");//javascript or JavaScript
	}
	
	@Override
	public void invoke(String function, String funcName, Object... args) {
		try {
			engine.eval(function);
			if (engine instanceof Invocable) {
				Invocable able = (Invocable)engine;
				double d=(double)able.invokeFunction(funcName, args);
				System.out.println(d);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		String function="function speedEconomy(spx){if(null !=spx && spx>0){return 4+(spx-80)*(spx-80)/1521;}return 4;}";
		new ScriptInvoke().invoke(function, "speedEconomy", 70);
	}

}


/*
 * 演示如何在java中如何通过线程来启动一个js方法
 *
  ScriptEngineManager sem=new ScriptEngineManager(); 
    ScriptEngine engine=sem.getEngineByName("js"); 
    String script="var obj=new Object();obj.run=function(){println('test thread')}"; 
    engine.eval(script); 
    Object obj=engine.get("obj");//获取js中对象 
    Invocable inv=(Invocable)engine; 
    Runnable r=inv.getInterface(obj,Runnable.class); 
    Thread t=new Thread(r); 
    t.start(); 
    
 *
 *
 *
  File file=new File("c:\\1.txt"); 
    engine.put("f", file); 
    try { 
        engine.eval("println('path:'+f.getPath())");//无法使用alert方法 
    } catch (ScriptException e) { 
        e.printStackTrace(); 
    } 
    
 *
 *JDK平台给script的方法中的形参赋值 
 *
  try 
    {  
      String script = "function say(){ return 'hello,'"+name+"; }"; 
      se.eval(script);  
      Invocable inv2 = (Invocable) se;  
      String res=(String)inv2.invokeFunction("say",name);  
      System.out.println(res); 
    }  
    catch(Exception e)  
    {  
        e.printStackTrace(); 
    }  
 *
 *调用函数
 *
 function merge(a, b) { 
 c = a * b; 
 return c; 
}
 if(engine instanceof Invocable) {    
Invocable invoke = (Invocable)engine;    // 调用merge方法，并传入两个参数    

// c = merge(2, 3);    

Double c = (Double)invoke.invokeFunction("merge", 2, 3)
 */