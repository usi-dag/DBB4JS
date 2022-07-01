package benchmarks.queries;

import SQL.dataset.TPCHStreamDB;



import java.util.List;

public interface Query_bench {
    TPCHStreamDB tpchStreamDB = TPCHStreamDB.get();

    List<?> benchStreamQuery();

    List<?> benchStreamQueryOpt();

    List<?> benchImperativeQuery();

}
