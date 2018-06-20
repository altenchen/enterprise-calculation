package storm.util;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Properties;

/**
 * 配置工具
 * @author xzp
 */
public final class ConfigUtils implements Serializable {
	private static final long serialVersionUID = 1920000001L;

	private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

	private static final ConfigUtils INSTANCE = new ConfigUtils();

	@Contract(pure = true)
    public static final ConfigUtils getInstance() {
	    return INSTANCE;
	}

	public final Properties sysDefine = new Properties();

	public final Properties sysParams = new Properties();

	{
        loadFromResource("sysDefine.properties", sysDefine);
        loadFromResource("parms.properties", sysParams);
	}

    private void loadFromResource(@NotNull String resourceName, @NotNull Properties properties) {
        InputStream in = ConfigUtils.class.getClassLoader().getResourceAsStream(resourceName);
        try {
            properties.load(new InputStreamReader(in, "UTF-8"));
            logger.info("从资源文件[" + resourceName + "]初始化配置成功");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
