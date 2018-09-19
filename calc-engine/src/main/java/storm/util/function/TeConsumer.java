package storm.util.function;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * @author xzp
 * 虽然都可以用高阶函数科里化来做, 但是没有函数这么直观, 所以加了这么个函数式接口.
 *
 * Represents an operation that accepts two input arguments and returns no
 * result.  This is the two-arity specialization of {@link Consumer}.
 * Unlike most other functional interfaces, {@code BiConsumer} is expected
 * to operate via side-effects.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #accept(Object, Object, Object)}.
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 * @param <F> the type of the third argument to the function
 *
 * @see Consumer
 * @since 1.8
 */
@FunctionalInterface
public interface TeConsumer<T, U, F> {

    /**
     * Performs this operation on the given arguments.
     *
     * @param t the first input argument
     * @param u the second input argument
     * @param f the third function argument
     */
    void accept(T t, U u, F f);

    /**
     * Returns a composed {@code BiConsumer} that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
     * operation throws an exception, it is relayed to the caller of the
     * composed operation.  If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after the operation to perform after this operation
     * @return a composed {@code BiConsumer} that performs in sequence this
     * operation followed by the {@code after} operation
     * @throws NullPointerException if {@code after} is null
     */
    default TeConsumer<T, U, F> andThen(TeConsumer<? super T, ? super U, ? super F> after) {

        Objects.requireNonNull(after);

        return (T t, U u, F f) -> {
            accept(t, u, f);
            after.accept(t, u, f);
        };
    }

    /**
     * Returns a composed function that first applies this function to
     * its input, and then applies the {@code after} function to the result.
     * If evaluation of either function throws an exception, it is relayed to
     * the caller of the composed function.
     *
     * @param <V> the type of output of the {@code after} function, and of the
     *           composed function
     * @param after the function to apply after this function is applied
     * @return a composed function that first applies this function and then
     * applies the {@code after} function
     * @throws NullPointerException if after is null
     */
    default <V> TeFunction<T, U, F, ? super V> andThen(TeFunction<? super T, ? super U, ? super F, ? extends V> after) {

        Objects.requireNonNull(after);

        return (T t, U u, F f) -> {
            accept(t, u, f);
            return after.apply(t, u, f);
        };
    }
}
