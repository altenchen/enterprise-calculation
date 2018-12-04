package storm.domain.fence.cron;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

/**
 * 计划基类, 实现了通用部分.
 * @author: xzp
 * @date: 2018-12-04
 * @description:
 * 多重计划, 只要其中任何一个处于激活状态, 则认为处于激活状态.
 */
public abstract class BaseCron implements Cron {

    @NotNull
    private final ImmutableCollection<Cron> cronSet;

    public BaseCron(
        @Nullable final ImmutableCollection<Cron> cronSet) {

        this.cronSet = Optional
            .ofNullable(cronSet)
            .orElseGet(
                () -> ImmutableSet.of(Cron.DEFAULT)
            );
    }

    @Override
    public final boolean active(final long time) {
        return cronSet.stream().anyMatch(cron -> cron.active(time));
    }
}
