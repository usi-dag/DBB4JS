package SQL.sql_utils;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;


public interface  GroupByCollector<T, A, R>  {
    Function<T, A> supplier();

    /**
     * A function that folds a value into a mutable result container.
     *
     * @return a function which folds a value into a mutable result container
     */
    BiConsumer<A, T> accumulator();

    /**
     * A function that accepts two partial results and merges them.  The
     * combiner function may fold state from one argument into the other and
     * return that, or may return a new result container.
     *
     * @return a function which combines two partial results into a combined
     * result
     */
    BinaryOperator<A> combiner();

    /**
     * Perform the final transformation from the intermediate accumulation type
     * {@code A} to the final result type {@code R}.
     *
     * <p>If the characteristic {@code IDENTITY_FINISH} is
     * set, this function may be presumed to be an identity transform with an
     * unchecked cast from {@code A} to {@code R}.
     *
     * @return a function which transforms the intermediate result to the final
     * result
     */
    Function<A, R> finisher();

    /**
     * Returns a {@code Set} of {@code Collector.Characteristics} indicating
     * the characteristics of this Collector.  This set should be immutable.
     *
     * @return an immutable set of collector characteristics
     */
    Set<Collector.Characteristics> characteristics();

    /**
     * Returns a new {@code Collector} described by the given {@code supplier},
     * {@code accumulator}, and {@code combiner} functions.  The resulting
     * {@code Collector} has the {@code Collector.Characteristics.IDENTITY_FINISH}
     * characteristic.
     *
     * @param supplier The supplier function for the new collector
     * @param accumulator The accumulator function for the new collector
     * @param combiner The combiner function for the new collector
     * @param characteristics The collector characteristics for the new
     *                        collector
     * @param <T> The type of input elements for the new collector
     * @param <R> The type of intermediate accumulation result, and final result,
     *           for the new collector
     * @throws NullPointerException if any argument is null
     * @return the new {@code Collector}
     */
    static<T, R> GroupByCollector<T, R, R> of(Function<T, R> supplier,
                                              BiConsumer<R, T> accumulator,
                                              BinaryOperator<R> combiner,
                                              Collector.Characteristics... characteristics) {
        Objects.requireNonNull(supplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(combiner);
        Objects.requireNonNull(characteristics);
        Set<Collector.Characteristics> cs = (characteristics.length == 0)
                ? SQLCollectors.CH_ID
                : Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH,
                characteristics));
        return new SQLCollectors.GroupByCollectorImpl<>(supplier, accumulator, combiner, cs);
    }

    static<T, A, R> GroupByCollector<T, A, R> of(Function<T, A> supplier,
                                                 BiConsumer<A, T> accumulator,
                                                 BinaryOperator<A> combiner,
                                                 Function<A, R> finisher) {
        Objects.requireNonNull(supplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(combiner);
        Set<Collector.Characteristics> cs = new HashSet<>();
        return new SQLCollectors.GroupByCollectorImpl<>(supplier, accumulator, combiner, finisher, cs);
    }
}