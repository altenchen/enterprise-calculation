package storm.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Properties;

/**
 * 配置工具
 */
public final class ConfigUtils implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1920000001L;
	public static final Properties sysDefine = new Properties();
	public static final Properties sysParams = new Properties();
	public static final Properties carTypeMapping = new Properties();
	static{
		InputStream in = null;
		try {
			in = ConfigUtils.class.getClassLoader().getResourceAsStream("sysDefine.properties");
			sysDefine.load(new InputStreamReader(in, "UTF-8"));

			in = ConfigUtils.class.getClassLoader().getResourceAsStream("parms.properties");
			sysParams.load(new InputStreamReader(in, "UTF-8"));

//			in = ConfigUtils.class.getClassLoader().getResourceAsStream("sysDefine.properties");
//			sysDefine.load(in);
//
//			in = ConfigUtils.class.getClassLoader().getResourceAsStream("parms.properties");
//			sysParams.load(in);
			
//			in = ConfigUtils.class.getClassLoader().getResourceAsStream("car_type_function_define.properties");
//			carTypeMapping.load(in);
			
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static Properties getSysDefine (){
		return sysDefine;
	}
	public static Properties getParams (){
		return sysParams;
	}
	public static Properties getCarTypeMapping (){
		return carTypeMapping;
	}
	public static void init(){}
}
