package ent.calc.test;

import ent.calc.util.ConfigUtils;
import org.apache.commons.collections.MapUtils;
import org.junit.jupiter.api.*;

/**
 * @author: xzp
 * @date: 2018-07-02
 * @description:
 */
@DisplayName("配置工具类测试")
public class ConfigUtilsTest {

    @Test
    @DisplayName("系统配置测试")
    public void testSysDefine() {
        final ConfigUtils configUtils = ConfigUtils.getInstance();
        Assertions.assertFalse(MapUtils.isEmpty(configUtils.sysDefine), "系统配置为空");
    }

    @Test
    @DisplayName("参数配置测试")
    public void testSysParams() {
        final ConfigUtils configUtils = ConfigUtils.getInstance();
        Assertions.assertFalse(MapUtils.isEmpty(configUtils.sysParams), "参数配置为空");
    }
}
