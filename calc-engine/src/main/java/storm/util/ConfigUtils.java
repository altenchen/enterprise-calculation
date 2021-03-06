package storm.util;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.conf.SysDefineEntity;
import storm.conf.SysParamEntity;
import storm.dao.DataToRedis;

import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 配置工具
 * 1步 properties从配置文件读取配置
 * 2步 从redis读取配置文件覆盖 第1步的配置 prepare 阶段
 * 3步 定时从redis读取新的配置进行覆盖     tick    阶段
 *
 * @author xzp
 */
public final class ConfigUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);

    private static SysDefineEntity sysDefine = new SysDefineEntity();
    private static SysParamEntity sysParam = new SysParamEntity();

    private static int REDIS_CACHE_DB = 4;
    private static String REDIS_CACHE_KEY = "conf.storm.env.param";
    //上一次从redis读取的时间
    private static long prevRedisReadTime = 0;
    //间隔60秒从redis读取一次
    private static long redisReadInterval = 60 * 1000;

    static {

        try {
            // 加载研发配置文件
            Properties sysDefine = new Properties();
            loadFromResource("sysDefine.properties", sysDefine);
            fillSysDefineEntity(sysDefine);
        } catch (IOException e) {
            e.printStackTrace();
            if (LOG.isWarnEnabled()) {
                LOG.error("{} 初始化失败.", ConfigUtils.class.getName());
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
                LOG.error("{} 初始化失败.", ConfigUtils.class.getName());
            }
        }
    }

    /**
     * 从redis读取配置
     */
    public synchronized static void readConfigFromRedis(DataToRedis redis) {
        if (redis == null) {
            LOG.error("REDIS 为null，未初始化");
            return;
        }
        long currentTime = System.currentTimeMillis();
        if (prevRedisReadTime > 0 && (currentTime - prevRedisReadTime) < redisReadInterval) {
            return;
        }
        prevRedisReadTime = currentTime;
        LOG.info("开始从REDIS读取配置 DB:{}, KEY:{}", REDIS_CACHE_DB, REDIS_CACHE_KEY);
        Map<String, String> redisConfig = redis.getMap(REDIS_CACHE_DB, REDIS_CACHE_KEY);
        if (MapUtils.isEmpty(redisConfig)) {
            LOG.info("REDIS 未配置任何参数 DB:{}, KEY:{}", REDIS_CACHE_DB, REDIS_CACHE_KEY);
            return;
        }
        fillSysDefineEntity(redisConfig);
        LOG.info("从REDIS读取配置完成 DB:{}, KEY:{}, 耗时: {} ms", REDIS_CACHE_DB, REDIS_CACHE_KEY, (System.currentTimeMillis() - currentTime));
    }

    /**
     * 将properties的值setg到sysDefine对象里面
    * @param properties
     */
    public static void fillSysDefineEntity(Properties properties) {
        if (properties.isEmpty()) {
            LOG.error("{} 未设置任何参数.", "sysDefine.properties");
            return;
        }
        for (Object paramKey : properties.keySet()) {
            String paramKeyString = paramKey + "";
            try {
                Object value = properties.get(paramKey);
                if (value == null) {
                    continue;
                }
                String beanAttributeName = keyConvertAttributeName(paramKeyString);
                BeanUtils.setProperty(sysDefine, beanAttributeName, properties.get(paramKey));
                LOG.info("应用配置 {}={}", paramKey, value);
            } catch (Exception e) {
                LOG.error("sysParamEntity 设置 beanAttributeName 出现异常, key:" + paramKey + ", value:" + properties.get(paramKey), e);
            }
        }
    }

    /**
     * 将properties的值setg到sysDefine对象里面
     * @param properties
     */
    public static void fillSysDefineEntity(Map<String, String> properties) {
        if (properties.isEmpty()) {
            LOG.error("{} 未设置任何参数.", "sysDefine.properties");
            return;
        }
        for (Object paramKey : properties.keySet()) {
            String paramKeyString = paramKey + "";
            try {
                Object value = properties.get(paramKey);
                if (value == null) {
                    continue;
                }
                String beanAttributeName = keyConvertAttributeName(paramKeyString);
                BeanUtils.setProperty(sysDefine, beanAttributeName, properties.get(paramKey));
                LOG.info("应用配置 {}={}", paramKey, value);
            } catch (Exception e) {
                LOG.error("sysParamEntity 设置 beanAttributeName 出现异常, key:" + paramKey + ", value:" + properties.get(paramKey), e);
            }
        }
    }

    /**
     * 将properties的值setg到sysParam对象里面
     * @param properties
     */
    private static void fillSysParamEntity(Properties properties) {
        if (properties.isEmpty()) {
            LOG.error("{} 未设置任何参数.", "params.properties");
            return;
        }
        for (Object paramKey : properties.keySet()) {
            String paramKeyString = paramKey + "";
            String beanAttributeName = keyConvertAttributeName(paramKeyString);
            try {
                Object value = properties.get(paramKey);
                if (StringUtils.isEmpty(value.toString())) {
                    continue;
                }
                BeanUtils.setProperty(sysParam, beanAttributeName, properties.get(paramKey));
                LOG.info("应用配置 {}={}", paramKey, value);
            } catch (Exception e) {
                LOG.error("sysParamEntity 设置 beanAttributeName 出现异常, key:" + paramKey + ", value:" + properties.get(paramKey), e);
            }
        }
    }

    /**
     * 所有properties中的key包含. _符号的都转换为首字母大写
     * 示例
     *     storm.worker.no 对应 stormWorkerNo
     *     storm.kafka.spout.no 对应 stormKafkaSpoutNo
     *     kafka.producer.vehicle_notice.topic 对应 kafkaProducerVehicleNoticeTopic
     * @param key
     * @return
     */
    private static String keyConvertAttributeName(String key) {
        Pattern p = Pattern.compile("(\\.|\\_)[a-zA-Z]{1}");
        Matcher m = p.matcher(key);
        while (m.find()) {
            String matchString = m.group();
            String upperCaseString = matchString.toUpperCase().substring(1);
            key = key.replace(matchString, upperCaseString);
        }
        return key;
    }

    /**
     * 加载properties文件
     * @param resourceName 配置文件名
     * @param properties
     * @throws IOException
     */
    private static void loadFromResource(@NotNull String resourceName, @NotNull Properties properties)
            throws IOException {

        final InputStream stream = ConfigUtils.class.getClassLoader().getResourceAsStream(resourceName);

        if (null == stream) {
            LOG.info("从资源文件初始化配置失败 {} ", resourceName);
            return;
        }

        LOG.info("从资源文件初始化配置开始 {} ", resourceName);

        final InputStreamReader reader = new InputStreamReader(stream, "UTF-8");
        properties.load(reader);

        LOG.info("从资源文件初始化配置完毕 {} ", resourceName);

        stream.close();
    }

    /**
     * 加载properties文件
     * @param file 配置文件名
     * @param properties
     * @throws IOException
     */
    public static void loadResourceFromLocal(File file, @NotNull Properties properties)
            throws IOException {

        final InputStream stream = new FileInputStream(file);

        if (null == stream) {
            LOG.info("从资源文件初始化配置失败 {} ", file.getName());
            return;
        }

        LOG.info("从资源文件初始化配置开始 {} ", file.getName());

        final InputStreamReader reader = new InputStreamReader(stream, "UTF-8");
        properties.load(reader);

        LOG.info("从资源文件初始化配置完毕 {} ", file.getName());

        stream.close();
    }

    public static SysDefineEntity getSysDefine() {
        return sysDefine;
    }

    public static SysParamEntity getSysParam() {
        return sysParam;
    }

}
