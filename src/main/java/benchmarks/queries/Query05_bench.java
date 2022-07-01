package benchmarks.queries;

import SQL.queries.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;

public class Query05_bench implements Query_bench{
    @Benchmark
    @Override
    public List<?> benchStreamQuery() {
        return new Query5().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchImperativeQuery() {
        return new Query5Imperative().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchStreamQueryOpt() {
        return new Query5SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQueryJoinMapMulti() {
        return new Query5JoinMapMulti().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySqlGroupBy() {
        return new Query5SqlGroupBy().execute(tpchStreamDB);
    }

    public static void main(String[] args) throws RunnerException {
        OptionBuilder.getRunner(Query05_bench.class.getSimpleName()).run();
    }
}
