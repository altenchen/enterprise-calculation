package storm.domain.fence.cron;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.CollectionUtils;
import org.jetbrains.annotations.Contract;
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

    private static final ImmutableSet<Cron> DEFAULT_CRON_SET = ImmutableSet.of(
        new Cron() {
            @Contract(pure = true)
            @Override
            public boolean active(final long time) {
                return true;
            }
        }
    );

    @NotNull
    private final ImmutableCollection<Cron> cronSet;

    public BaseCron(
        @Nullable final ImmutableCollection<Cron> cronSet) {

        if (CollectionUtils.isEmpty(cronSet)) {
            this.cronSet = DEFAULT_CRON_SET;
        } else {
            this.cronSet = cronSet;
        }
    }

    @Override
    public final boolean active(final long time) {
        return cronSet.stream().anyMatch(cron -> cron.active(time));
    }
}
