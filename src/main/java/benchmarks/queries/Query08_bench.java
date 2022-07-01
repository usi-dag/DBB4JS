package benchmarks.queries;


import SQL.queries.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;

public class Query08_bench implements Query_bench {

    @Benchmark
    @Override
    public List<?> benchStreamQuery() {
        return new Query8().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchImperativeQuery() {
        return new Query8Imperative().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchStreamQueryOpt() {
        return new Query8SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQueryJoinMapMulti(){
        return new Query8JoinMapMulti().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySqlGroupBy() {
        return new Query8SqlGroupBy().execute(tpchStreamDB);
    }

    public static void main(String[] args) throws RunnerException {
        OptionBuilder.getRunner(Query08_bench.class.getSimpleName()).run();
    }
}
