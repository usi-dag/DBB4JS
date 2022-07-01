package benchmarks.queries;

import SQL.queries.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;

@State(Scope.Thread)
public class Query06_bench implements Query_bench{

    @Benchmark
    @Override
    public List<?> benchStreamQuery() {
        return new Query6().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchImperativeQuery() {return new Query6Imperative().execute(tpchStreamDB);}

    @Benchmark
    @Override
    public List<?> benchStreamQueryOpt() {
        return new Query6SingleFilter().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySingleFilter() {
        return new Query6SingleFilter().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQueryParallel() {
        return new Query6Parallel().execute(tpchStreamDB);
    }

    public static void main(String[] args) throws RunnerException {
        OptionBuilder.getRunner(Query06_bench.class.getSimpleName()).run();
    }
}
