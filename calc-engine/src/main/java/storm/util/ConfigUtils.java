package storm.util;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.conf.SysDefineEntity;
import storm.conf.SysParamEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 配置工具
 * 1步 properties从配置文件读取配置
 * 2步 从redis读取配置文件覆盖 第1步的配置     未实现
 * 3步 定时从redis读取新的配置进行覆盖         未实现
 *
 * @author xzp
 */
public final class ConfigUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);

    private static SysDefineEntity sysDefine = new SysDefineEntity();
    private static SysParamEntity sysParam = new SysParamEntity();

    static{

        try {
            // 加载研发配置文件
            Properties sysDefine = new Properties();
            loadFromResource("sysDefine.properties", sysDefine);
            fillSysDefineEntity(sysDefine);
        } catch (IOException e) {
            e.printStackTrace();
            if (LOG.isWarnEnabled()) {
                LOG.error("[{}]初始化失败.", ConfigUtils.class.getName());
            }
        }

        try {
            // 加载运维配置文件
            Properties sysParams = new Properties();
            loadFromResource("parms.properties", sysParams);
            fillSysParamEntity(sysParams);
        } catch (IOException e) {
            e.printStackTrace();
            if (LOG.isWarnEnabled()) {
                LOG.error("[{}]初始化失败.", ConfigUtils.class.getName());
            }
        }
    }

    private static void fillSysDefineEntity(Properties properties) {
        if( properties.isEmpty() ){
            LOG.error("{} 未设置任何参数.", "sysDefine.properties");
            return;
        }
        for( Object paramKey : properties.keySet() ){
            String paramKeyString = paramKey + "";
            String beanAttributeName = keyConvertAttributeName(paramKeyString);
            try {
                Object value = properties.get(paramKey);
                if(value == null){
                    continue;
                }
                BeanUtils.setProperty(sysDefine, beanAttributeName, properties.get(paramKey));
            } catch (Exception e) {
                LOG.error("sysParamEntity 设置 beanAttributeName 出现异常, key = " + paramKey + ", value = " + properties.get(paramKey), e);
            }
        }
    }

    private static void fillSysParamEntity(Properties properties) {
        if( properties.isEmpty() ){
            LOG.error("{} 未设置任何参数.", "params.properties");
            return;
        }
        for( Object paramKey : properties.keySet() ){
            String paramKeyString = paramKey + "";
            String beanAttributeName = keyConvertAttributeName(paramKeyString);
            try {
                Object value = properties.get(paramKey);
                if(StringUtils.isEmpty(value.toString())){
                    continue;
                }
                BeanUtils.setProperty(sysParam, beanAttributeName, properties.get(paramKey));
            } catch (Exception e) {
                LOG.error("sysParamEntity 设置 beanAttributeName 出现异常, key = " + paramKey + ", value = " + properties.get(paramKey), e);
            }
        }
    }

    //properties 文件里的key转类属性名
    private static String keyConvertAttributeName(String key){
        Pattern p = Pattern.compile("(\\.|\\_)[a-zA-Z]{1}");
        Matcher m = p.matcher(key);
        while (m.find()) {
            String matchString = m.group();
            String upperCaseString = matchString.toUpperCase().substring(1);
            key = key.replace(matchString, upperCaseString);
        }
        return key;
    }

    //加载properties文件
    private static void loadFromResource(@NotNull String resourceName, @NotNull Properties properties)
            throws IOException {

        final InputStream stream = ConfigUtils.class.getClassLoader().getResourceAsStream(resourceName);

        if (null == stream) {
            LOG.info("从资源文件初始化配置失败[{}]", resourceName);
            return;
        }

        LOG.info("从资源文件初始化配置开始[{}]", resourceName);

        final InputStreamReader reader = new InputStreamReader(stream, "UTF-8");
        properties.load(reader);

        LOG.info("从资源文件初始化配置完毕[{}]", resourceName);

        stream.close();
    }

    public static SysDefineEntity getSysDefine() {
        return sysDefine;
    }

    public static SysParamEntity getSysParam(){
        return sysParam;
    }
}
