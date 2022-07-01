package benchmarks.queries;


import SQL.queries.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;

public class Query19_bench implements Query_bench {

    @Benchmark
    @Override
    public List<?> benchStreamQuery() {
        return new Query19().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchImperativeQuery() {
        return new Query19Imperative().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchStreamQueryOpt() {
        return new Query19SingleFilterAndJoinMapMulti().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQueryJoinMapMulti() {
        return new Query19JoinMapMulti().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySingleFilter() {
        return new Query19SingleFilter().execute(tpchStreamDB);
    }

    public static void main(String[] args) throws RunnerException {
        OptionBuilder.getRunner(Query19_bench.class.getSimpleName()).run();
    }
}
