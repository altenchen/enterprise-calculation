package storm.domain.fence.area;

import com.google.common.collect.ImmutableCollection;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import storm.domain.fence.cron.BaseCron;
import storm.domain.fence.cron.Cron;

/**
 * 区域基类, 实现了通用部分.
 * @author: xzp
 * @date: 2018-12-04
 * @description:
 * 每个区域可以包含多个激活时间段
 */
public abstract class BaseArea extends BaseCron implements Area, Cron {

    @NotNull
    private final String areaId;

    BaseArea(
        @NotNull final String areaId,
        @Nullable final ImmutableCollection<Cron> cronSet) {

        super(cronSet);
        this.areaId = areaId;
    }

    @Contract(pure = true)
    @NotNull
    @Override
    public final String getAreaId() {
        return areaId;
    }
}
