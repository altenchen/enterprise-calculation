package storm.stream;

import java.io.Serializable;

/**
 * @author: xzp
 * @date: 2018-09-07
 * @description: Spout 消息 Id 包装类, 主要是为了 emit 时与元组的成员区分开来
 */
public final class MessageId<T>{

    public final T value;

    public MessageId(final T value) {
        this.value = value;
    }
}
