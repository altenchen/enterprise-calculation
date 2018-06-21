package storm.util;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
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

	public final Properties sysDefine = new Properties();

	public final Properties sysParams = new Properties();

	{

	    if(INSTANCE != null) {
	        throw new IllegalStateException();
        }

        try {
            loadFromResource("sysDefine.properties", sysDefine);
            loadFromResource("parms.properties", sysParams);
        } catch (IOException e) {
            e.printStackTrace();
            if(logger.isWarnEnabled()) {
                logger.error("[" + ConfigUtils.class.getName() + "]初始化失败.");
            }
        }
	}

    private void loadFromResource(@NotNull String resourceName, @NotNull Properties properties)
        throws IOException {

        InputStream stream = ConfigUtils.class.getClassLoader().getResourceAsStream(resourceName);

        if(null == stream) {
            if (logger.isInfoEnabled()) {
                logger.info("从资源文件初始化配置失败[" + resourceName + "]");
            }
            return;
        }

        if (logger.isInfoEnabled()) {
            logger.info("从资源文件初始化配置开始[" + resourceName + "]");
        }

        final InputStreamReader reader = new InputStreamReader(stream, "UTF-8");
        properties.load(reader);

        if (logger.isInfoEnabled()) {
            logger.info("从资源文件初始化配置完毕[" + resourceName + "]");
        }

        stream.close();
    }
}
