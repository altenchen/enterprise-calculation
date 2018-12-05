package storm.domain.fence.event;

/**
 * 事件阶段
 * @author: xzp
 * @date: 2018-12-05
 * @description:
 */
public enum EventStage {

    /**
     * 未知
     */
    UNKNOWN,

    /**
     * 开始
     */
    BEGIN,

    /**
     * 持续
     */
    CONTINUE,

    /**
     * 结束
     */
    END
}
