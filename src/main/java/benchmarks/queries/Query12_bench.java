package benchmarks.queries;


import SQL.queries.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;

@State(Scope.Thread)
public class Query12_bench implements Query_bench {
    @Benchmark
    @Override
    public List<?> benchStreamQuery() {
        return new Query12().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchImperativeQuery() {
        return new Query12Imperative().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchStreamQueryOpt() {
        return new Query12SingleFilterAndJoinMapMultiAndSqlGroupBy().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySingleFilter() {
        return new Query12SingleFilter().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySqlGroupBy() {
        return new Query12SqlGroupBy().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQueryJoinMapMulti() {
        return new Query12JoinMapMulti().execute(tpchStreamDB);
    }

    public static void main(String[] args) throws RunnerException {
        OptionBuilder.getRunner(Query12_bench.class.getSimpleName()).run();
    }
}
