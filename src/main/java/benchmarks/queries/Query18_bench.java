package benchmarks.queries;

import SQL.queries.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;

public class Query18_bench implements Query_bench {

    @Benchmark
    @Override
    public List<?> benchStreamQuery() {
        return new Query18().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchImperativeQuery() {
        return new Query18Imperative().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchStreamQueryOpt() {
        return new Query18SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);
    }

//    @Benchmark
    public List<?> benchImperativeQueryLoopFusion() {
        return new Query18ImperativeLoopFusion().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySqlGroupBy() {
        return new Query18SqlGroupBy().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQueryJoinMapMulti() {
        return new Query18JoinMapMulti().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQueryParallel() {
        return new Query18Parallel().execute(tpchStreamDB);
    }

    public static void main(String[] args) throws RunnerException {
        OptionBuilder.getRunner(Query18_bench.class.getSimpleName()).run();
    }
}
