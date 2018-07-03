package ent.calc.util;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * 配置工具
 * @author xzp
 */
public final class ConfigUtils {

	private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    private static final ConfigUtils INSTANCE = new ConfigUtils();

	@Contract(pure = true)
    public static ConfigUtils getInstance() {
	    return INSTANCE;
	}

    /**
     * 运维配置参数
     */
	@NotNull
	public final Properties sysDefine = new Properties();

    /**
     * 研发配置参数
     */
	@NotNull
	public final Properties sysParams = new Properties();

	{

	    if(INSTANCE != null) {
	        throw new IllegalStateException();
        }

        try {
	        // 加载研发配置文件
            loadFromResource("sysDefine.properties", sysDefine);
            // 加载运维配置文件
            loadFromResource("parms.properties", sysParams);
        } catch (IOException e) {
            e.printStackTrace();
            if(logger.isWarnEnabled()) {
                logger.error("[{}]初始化失败.", ConfigUtils.class.getName());
            }
        }
	}

	private ConfigUtils() {}

    private void loadFromResource(@NotNull String resourceName, @NotNull Properties properties)
        throws IOException {

        final InputStream stream = ConfigUtils.class.getClassLoader().getResourceAsStream(resourceName);

        if(null == stream) {
            logger.info("从资源文件初始化配置失败[{}]", resourceName);
            return;
        }

        logger.info("从资源文件初始化配置开始[{}]", resourceName);

        final InputStreamReader reader = new InputStreamReader(stream, "UTF-8");
        properties.load(reader);

        logger.info("从资源文件初始化配置完毕[{}]", resourceName);

        stream.close();
    }
}
