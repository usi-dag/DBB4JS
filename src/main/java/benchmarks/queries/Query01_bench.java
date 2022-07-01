package benchmarks.queries;

import SQL.queries.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;

@State(Scope.Thread)
public class Query01_bench implements Query_bench{

    @Benchmark
    @Override
    public List<?> benchStreamQuery() {
        return new Query1().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchImperativeQuery() {
        return new Query1Imperative().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchStreamQueryOpt() {
        return new Query1SqlGroupBy().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySqlGroupBy() {
        return new Query1SqlGroupBy().execute(this.tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQueryParallel() {
        return new Query1Parallel().execute(tpchStreamDB);
    }


    public static void main(String[] args) throws RunnerException {
        OptionBuilder.getRunner(Query01_bench.class.getSimpleName()).run();
    }

}
