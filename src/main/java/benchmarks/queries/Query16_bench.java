package benchmarks.queries;

import SQL.queries.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;

public class Query16_bench implements Query_bench {

    @Benchmark
    @Override
    public List<?> benchStreamQuery() {
        return new Query16().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchImperativeQuery() {
        return new Query16Imperative().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchStreamQueryOpt() {
        return new Query16SqlGroupByAndJoinMapMultiAndSingleFilter().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySingleFilter() {
        return new Query16SingleFilter().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQueryJoinMapMulti() {
        return new Query16JoinMapMulti().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySqlGroupBy() {
        return new Query16SqlGroupBy().execute(tpchStreamDB);
    }

    public static void main(String[] args) throws RunnerException {
        OptionBuilder.getRunner(Query16_bench.class.getSimpleName()).run();
    }

}
