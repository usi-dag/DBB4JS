package benchmarks.queries;

import SQL.queries.*;
import org.openjdk.jmh.annotations.Benchmark;

import java.util.List;

public class Query17_bench implements Query_bench{

    @Benchmark
    @Override
    public List<?> benchStreamQuery() {
        return new Query17().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchImperativeQuery() {
        return new Query17Imperative().execute(tpchStreamDB);
    }

    @Benchmark
    @Override
    public List<?> benchStreamQueryOpt() {
        return new Query17JoinMapMultiAndSingleFilterAndSqlGroupBy().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySqlGroupBy() {
        return new Query17SqlGroupBy().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQuerySingleFilter() {
        return new Query17SingleFilter().execute(tpchStreamDB);
    }

    @Benchmark
    public List<?> benchStreamQueryJoinMapMulti() {
        return new Query17JoinMapMulti().execute(tpchStreamDB);
    }

}
