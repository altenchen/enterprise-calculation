package storm.dto.alarm;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.util.ConfigUtils;

/**
 * @author 徐志鹏
 * 偏移系数自定义数据项处理
 *
 * TODO: 目前从配置文件读, 等基础平台规划好, 将会改成从数据库读.
 */
public final class CoefficientOffsetGetter {

    @NotNull
    private static final Logger LOG = LoggerFactory.getLogger(CoefficientOffsetGetter.class);

    /**
     * 偏移系数项 <dataKey, dataKey>, 未配置的数据项将无法参与计算.
     */
    @NotNull
    private static final ImmutableMap<String, CoefficientOffset> FIXED_COEFFICIENT_OFFSETS;

    static {
        FIXED_COEFFICIENT_OFFSETS = initFromResource();
    }

    @NotNull
    private static ImmutableMap<String, CoefficientOffset> initFromResource() {

        final String resourceName = "coefficient_offset.json";
        final InputStream stream = ConfigUtils.class.getClassLoader().getResourceAsStream(resourceName);
        if(null == stream) {
            LOG.error("从资源文件初始化偏移系数失败[{}]", resourceName);
            return ImmutableMap.of();
        }

        LOG.info("从资源文件初始化偏移系数开始[{}]", resourceName);

        try {
            final ArrayList<CoefficientOffset> coefficientOffsets =  JSON.parseObject(
                stream,
                Charset.forName("UTF-8"),
                new TypeToken<ArrayList<CoefficientOffset>>() {
                }.getType()
            );
            if(CollectionUtils.isNotEmpty(coefficientOffsets)) {
                final HashMap<String, CoefficientOffset> coefficientOffsetMap = Maps.newHashMapWithExpectedSize(coefficientOffsets.size());
                coefficientOffsets.forEach(coefficientOffset -> {
                    final String key = coefficientOffset.getDataKey();
                    if (coefficientOffsetMap.containsKey(key)) {
                        LOG.warn("冲突的偏移系数配置[{}]", key);
                    }
                    coefficientOffsetMap.put(key, coefficientOffset);
                });
                return new ImmutableMap.Builder<String, CoefficientOffset>()
                    .putAll(coefficientOffsetMap)
                    .build();
            } else {
                LOG.error("偏移系数规则为空");
                return ImmutableMap.of();
            }
        } catch (@NotNull final IOException e) {
            LOG.error("从资源文件初始化偏移系数异常", e);
            return ImmutableMap.of();
        }
    }

    @Contract(pure = true)
    public static ImmutableMap<String, CoefficientOffset> getCoefficientOffsets() {
        return FIXED_COEFFICIENT_OFFSETS;
    }

    @Nullable
    public static CoefficientOffset getCoefficientOffset(
        @Nullable final String dataKey) {

        if (StringUtils.isBlank(dataKey)) {
            return null;
        }

        return FIXED_COEFFICIENT_OFFSETS.get(dataKey);
    }
}
