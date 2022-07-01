package SQL.sql_utils;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SQLStreamUtils {

    // Group By Operator

    public static <T, K, V, F> Stream<F> streamGroupBy(Stream<T> rows,
                                                       Function<T, K> keyGetter,
                                                       GroupByCollector<T, V, F> groupByCollector) {
        return rows
                .collect(SQLCollectors.groupingBy(keyGetter, groupByCollector))
                .values()
                .stream();
    }


    public static <T, K, V, M, F> Stream<M> streamGroupBy(Stream<T> rows,
                                                          Function<T, K> keyGetter,
                                                          Collector<T, V, F> groupByCollector,
                                                          BiFunction<K, F ,M> mapper) {
        return rows
                .collect(Collectors.groupingBy(keyGetter, groupByCollector))
                .entrySet()
                .stream()
                .map(entry -> mapper.apply(entry.getKey(), entry.getValue()));
    }

    // Join Operators

    /**
     * Stream Join Operator
     * note: currently it always builds on the left side and probe on the right side
     * */
    public static <L, R, K, J> Stream<J> streamJoin(Stream<L> left,
                                                    Function<L, K> leftKeyGetter,
                                                    Stream<R> right,
                                                    Function<R, K> rightKeyGetter,
                                                    BiFunction<L, R, J> mapping) {

        List<L> empty = Collections.emptyList();

        // build hashmap for left side (build) stream
        Map<K, List<L>> map = left.collect(Collectors.groupingBy(leftKeyGetter));

        // probe side
        return right.flatMap(r ->
                map.getOrDefault(rightKeyGetter.apply(r), empty)
                        .stream()
                        .map(l -> mapping.apply(l, r)));
    }


    /**
     * Stream Join Operator + filter
     * note: currently it always builds on the left side and probe on the right side
     * */
    public static <L, R, K, J> Stream<J> streamJoinFilter(Stream<L> left,
                                                          Function<L, K> leftKeyGetter,
                                                          Stream<R> right,
                                                          Function<R, K> rightKeyGetter,
                                                          BiFunction<L, R, J> mapping,
                                                          BiPredicate<L,R> preFilter) {

        List<L> empty = Collections.emptyList();

        // build hashmap for left side (build) stream
        Map<K, List<L>> map = left.collect(Collectors.groupingBy(leftKeyGetter));

        // probe side
        return right.flatMap(r ->
                map.getOrDefault(rightKeyGetter.apply(r), empty)
                        .stream()
                        .filter((l) -> preFilter.test(l, r))
                        .map(l -> mapping.apply(l, r)));
    }

    /**
     * Stream Join Operator
     * note: currently it always builds on the left side and probe on the right side
     * */
    public static <L, R, K, J> Stream<J> streamJoinMapMulti(Stream<L> left,
                                                            Function<L, K> leftKeyGetter,
                                                            Stream<R> right,
                                                            Function<R, K> rightKeyGetter,
                                                            BiFunction<L, R, J> mapping) {

        // build hashmap for left side (build) stream
        Map<K, List<L>> map = left.collect(Collectors.groupingBy(leftKeyGetter));

        // probe side
        return right.mapMulti((r, consumer) -> {
            var lefts = map.get(rightKeyGetter.apply(r));
            if(lefts != null) {
                lefts.forEach(l -> consumer.accept(mapping.apply(l, r)));
            }
        });
    }

    /**
     * Stream Join Operator
     * note: currently it always builds on the left side and probe on the right side
     * */
    public static <L, R, K, J> Stream<J> streamJoinMapMultiFilter(Stream<L> left,
                                                                  Function<L, K> leftKeyGetter,
                                                                  Stream<R> right,
                                                                  Function<R, K> rightKeyGetter,
                                                                  BiFunction<L, R, J> mapping,
                                                                  BiPredicate<L,R> preFilter) {

        // build hashmap for left side (build) stream
        Map<K, List<L>> map = left.collect(Collectors.groupingBy(leftKeyGetter));

        // probe side
        return right.mapMulti((r, consumer) -> {
            var lefts = map.get(rightKeyGetter.apply(r));
            if(lefts != null) {
                lefts.forEach(l -> {
                    if(preFilter.test(l, r)) {
                        consumer.accept(mapping.apply(l, r));
                    }
                });
            }
        });
    }


    public static <T,A extends Comparable<? super A>,B extends Comparable<? super B>> Comparator<T> comparator(Function<T,A> a, Function<T,B> b){
        return Comparator.comparing(a).thenComparing(b);
    }


    public static <L,R,K> Stream<L> streamLeftSemiJoin(Stream<L> left,
                                                       Function<L, K> leftKeyGetter,
                                                       Stream<R> right,
                                                       Function<R, K> rightKeyGetter) {

        Map<K, MarkedArrayList<L>> map = left.collect(Collectors.groupingBy(leftKeyGetter, SQLCollectors.toMarkedList()));
        return right.mapMulti((r, c) -> {
            MarkedArrayList<L> lst = map.get(rightKeyGetter.apply(r));
            if(lst != null && !lst.isMarked()) {
                lst.mark();
                lst.forEach(c);
            }
        });
    }

    public static <L,R,K> Stream<L> streamLeftSemiJoinFilter(Stream<L> left,
                                                             Function<L, K> leftKeyGetter,
                                                             Stream<R> right,
                                                             Function<R, K> rightKeyGetter,
                                                             BiPredicate<L,R> preFilter) {

        Map<K, List<MarkedRowWrapper<L>>> map = left
                .map(MarkedRowWrapper::new)
                .collect(Collectors.groupingBy(row -> leftKeyGetter.apply(row.element())));
        return right.mapMulti((rightRow, c) -> {
            List<MarkedRowWrapper<L>> lst = map.get(rightKeyGetter.apply(rightRow));
            if(lst != null) {
                lst.forEach(leftRow -> {
                    if(preFilter.test(leftRow.element(), rightRow) && !leftRow.isMarked()) {
                        leftRow.mark();
                        c.accept(leftRow.element());
                    }
                });
            }
        });
    }

    public static <L,R,K> Stream<R> streamRightSemiJoin(Stream<L> left,
                                                        Function<L, K> leftKeyGetter,
                                                        Stream<R> right,
                                                        Function<R, K> rightKeyGetter) {

        var toRem = left.map(leftKeyGetter).collect(Collectors.toSet());
        return right.filter(r -> toRem.contains(rightKeyGetter.apply(r)));
    }
    public static <L,R,K> Stream<R> streamRightAntiJoin(Stream<L> left,
                                                        Function<L, K> leftKeyGetter,
                                                        Stream<R> right,
                                                        Function<R, K> rightKeyGetter) {

        var toRem = left.map(leftKeyGetter).collect(Collectors.toSet());
        return right.filter(r -> !toRem.contains(rightKeyGetter.apply(r)));
    }

    public static <L,R,K> Stream<L> streamLeftAntiJoin(Stream<L> left,
                                                       Function<L, K> leftKeyGetter,
                                                       Stream<R> right,
                                                       Function<R, K> rightKeyGetter) {

        Map<K, List<MarkedRowWrapper<L>>> map = left
                .map(MarkedRowWrapper::new)
                .collect(Collectors.groupingBy(row -> leftKeyGetter.apply(row.element())));

        right.forEach(rightRow -> {
            List<MarkedRowWrapper<L>> lst = map.get(rightKeyGetter.apply(rightRow));
            if(lst != null) {
                lst.forEach(MarkedRowWrapper::mark);
            }
        });

        return map.values().stream().mapMulti((ls, consumer) -> {
            ls.forEach(leftRow -> {
                if (!leftRow.isMarked()) {
                    consumer.accept(leftRow.element());
                }
            });
        });
    }


    public static <L,R,K> Stream<L> streamLeftAntiJoinFilter(Stream<L> left,
                                                             Function<L, K> leftKeyGetter,
                                                             Stream<R> right,
                                                             Function<R, K> rightKeyGetter,
                                                             BiPredicate<L,R> preFilter) {
        Map<K, List<MarkedRowWrapper<L>>> map = left
                .map(MarkedRowWrapper::new)
                .collect(Collectors.groupingBy(row -> leftKeyGetter.apply(row.element())));

        right.forEach((rightRow) -> {
            List<MarkedRowWrapper<L>> lst = map.get(rightKeyGetter.apply(rightRow));
            if(lst != null) {
                lst.stream()
                        .filter(leftRow -> preFilter.test(leftRow.element(), rightRow))
                        .forEach(MarkedRowWrapper::mark);
            }
        });

        return map.values().stream().mapMulti((ls, consumer) -> {
            ls.stream().filter(mRow -> !mRow.isMarked()).forEach(leftRow -> {
                consumer.accept(leftRow.element());
            });
        });
    }
}
