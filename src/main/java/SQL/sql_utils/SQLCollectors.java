package SQL.sql_utils;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class SQLCollectors {
    static final Set<Collector.Characteristics> CH_ID
            = Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH));
    static final Set<Collector.Characteristics> CH_NOID = Collections.emptySet();


    public static <T> Collector<T, ?, MarkedArrayList<T>> toMarkedList() {
        return Collector.of(
                MarkedArrayList::new,
                List::add,
                (left, right) -> { left.addAll(right); return left; },
                Collector.Characteristics.IDENTITY_FINISH);
    }


    public static <T, K, A, D>
    Collector<T, ?, Map<K, D>> groupingBy(Function<? super T, ? extends K> classifier,
                                          GroupByCollector<? super T, A, D> downstream) {
        return groupingBy(classifier, HashMap::new, downstream);
    }

    public static <T, K, D, A, M extends Map<K, D>>
    Collector<T, ?, M> groupingBy(Function<? super T, ? extends K> classifier,
                                  Supplier<M> mapFactory,
                                  GroupByCollector<? super T, A, D> downstream) {
        Function<? super T, A> downstreamSupplier = downstream.supplier();
        BiConsumer<A, ? super T> downstreamAccumulator = downstream.accumulator();
        BiConsumer<Map<K, A>, T> accumulator = (m, t) -> {
            K key = Objects.requireNonNull(classifier.apply(t), "element cannot be mapped to a null key");
            A container = m.computeIfAbsent(key, k -> downstreamSupplier.apply(t));
            downstreamAccumulator.accept(container, t);
        };
        BinaryOperator<Map<K, A>> merger = mapMerger(downstream.combiner());
        @SuppressWarnings("unchecked")
        Supplier<Map<K, A>> mangledFactory = (Supplier<Map<K, A>>) mapFactory;

        if (downstream.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
            return new CollectorImpl<>(mangledFactory, accumulator, merger);
        }
        else {
            @SuppressWarnings("unchecked")
            Function<A, A> downstreamFinisher = (Function<A, A>) downstream.finisher();
            Function<Map<K, A>, M> finisher = intermediate -> {
                intermediate.replaceAll((k, v) -> downstreamFinisher.apply(v));
                @SuppressWarnings("unchecked")
                M castResult = (M) intermediate;
                return castResult;
            };
            return new CollectorImpl<>(mangledFactory, accumulator, merger, finisher, CH_NOID);
        }
    }


    static class GroupByCollectorImpl<T, A, R> implements GroupByCollector<T, A, R> {
        private final Function<T, A> supplier;
        private final BiConsumer<A, T> accumulator;
        private final BinaryOperator<A> combiner;
        private final Function<A, R> finisher;
        private final Set<Collector.Characteristics> characteristics;

        GroupByCollectorImpl(Function<T, A> supplier,
                             BiConsumer<A, T> accumulator,
                             BinaryOperator<A> combiner,
                             Function<A,R> finisher,
                             Set<Collector.Characteristics> characteristics) {
            this.supplier = supplier;
            this.accumulator = accumulator;
            this.combiner = combiner;
            this.finisher = finisher;
            this.characteristics = characteristics;
        }

        GroupByCollectorImpl(Function<T, A> supplier,
                             BiConsumer<A, T> accumulator,
                             BinaryOperator<A> combiner,
                             Set<Collector.Characteristics> characteristics) {
            this(supplier, accumulator, combiner, castingIdentity(), characteristics);
        }

        @Override
        public BiConsumer<A, T> accumulator() {
            return accumulator;
        }

        @Override
        public Function<T, A> supplier() {
            return supplier;
        }

        @Override
        public BinaryOperator<A> combiner() {
            return combiner;
        }

        @Override
        public Function<A, R> finisher() {
            return finisher;
        }

        @Override
        public Set<Collector.Characteristics> characteristics() {
            return characteristics;
        }
    }

    static class CollectorImpl<T, A, R> implements Collector<T, A, R> {
        private final Supplier<A> supplier;
        private final BiConsumer<A, T> accumulator;
        private final BinaryOperator<A> combiner;
        private final Function<A, R> finisher;
        private final Set<Characteristics> characteristics;

        CollectorImpl(Supplier<A> supplier,
                      BiConsumer<A, T> accumulator,
                      BinaryOperator<A> combiner,
                      Function<A,R> finisher,
                      Set<Characteristics> characteristics) {
            this.supplier = supplier;
            this.accumulator = accumulator;
            this.combiner = combiner;
            this.finisher = finisher;
            this.characteristics = characteristics;
        }

        CollectorImpl(Supplier<A> supplier,
                      BiConsumer<A, T> accumulator,
                      BinaryOperator<A> combiner) {
            this(supplier, accumulator, combiner, castingIdentity(), SQLCollectors.CH_ID);
        }

        @Override
        public BiConsumer<A, T> accumulator() {
            return accumulator;
        }

        @Override
        public Supplier<A> supplier() {
            return supplier;
        }

        @Override
        public BinaryOperator<A> combiner() {
            return combiner;
        }

        @Override
        public Function<A, R> finisher() {
            return finisher;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return characteristics;
        }
    }

    @SuppressWarnings("unchecked")
    private static <I, R> Function<I, R> castingIdentity() {
        return i -> (R) i;
    }

    private static <K, V, M extends Map<K,V>>
    BinaryOperator<M> mapMerger(BinaryOperator<V> mergeFunction) {
        return (m1, m2) -> {
            for (Map.Entry<K,V> e : m2.entrySet())
                m1.merge(e.getKey(), e.getValue(), mergeFunction);
            return m1;
        };
    }
}
