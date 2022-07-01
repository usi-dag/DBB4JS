package benchmarks.queries;

import SQL.queries.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;

public class Query02_bench implements Query_bench{

    @Benchmark
    @Override
    public List<?> benchStreamQuery() {
        return new Query2().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchImperativeQuery() {
        return new Query2Imperative().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchStreamQueryOpt() {
        return new Query2SingleFilterAndJoinMapMulti().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySingleFilter() {
        return new Query2SingleFilter().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQueryJoinMapMulti() {
        return new Query2JoinMapMulti().execute(tpchStreamDB);
    }


    public static void main(String[] args) throws RunnerException {
        OptionBuilder.getRunner(Query02_bench.class.getSimpleName()).run();
    }
}
