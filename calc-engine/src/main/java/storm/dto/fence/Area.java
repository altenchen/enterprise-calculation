package storm.dto.fence;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

/**
 * 区域
 * @author: xzp
 * @date: 2018-11-28
 * @description:
 */
public interface Area {

    /**
     * 判断坐标在区域的内部或者外部
     * @param coordinate 坐标
     * @param distance 坐标与边界的缓冲距离, 输入应该为零或正数..
     * @return true-在内部, false-在外部, null 在缓冲距离内
     */
    @Nullable
    Boolean whichSide(@NotNull final Coordinate coordinate, final double distance);
}
