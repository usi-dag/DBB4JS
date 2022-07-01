package benchmarks.queries;

import SQL.queries.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;

public class Query22_bench implements Query_bench {

    @Benchmark
    @Override
    public List<?> benchStreamQuery() {
        return new Query22().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchImperativeQuery() {
        return new Query22Imperative().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchStreamQueryOpt() {
        return new Query22SingleFilterAndSqlGroupBy().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySingleFilter() {
        return new Query22SingleFilter().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySqlGroupBy() {
        return new Query22SqlGroubBy().execute(tpchStreamDB);
    }


    public static void main(String[] args) throws RunnerException {
        OptionBuilder.getRunner(Query22_bench.class.getSimpleName()).run();
    }
}
