package storm.domain.fence.area;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.domain.fence.cron.Cron;

import java.util.Objects;

/**
 * 区域接口
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 */
public interface Area {

    /**
     * 获取区域标识
     * @return 区域标识
     */
    @NotNull
    String getAreaId();

    /**
     * 计算定位坐标于区域拓扑关系
     * @param coordinate 坐标
     * @param inSideDistance 坐标与边界的缓冲距离, 输入应该为零或正数
     * @param outsideDistance 坐标与边界的缓冲距离, 输入应该为零或正数
     * @return true-在内部, false-在外部, null-在边界
     */
    @NotNull
    AreaSide computeAreaSide(
        @NotNull final Coordinate coordinate,
        final double inSideDistance,
        final double outsideDistance);
}
