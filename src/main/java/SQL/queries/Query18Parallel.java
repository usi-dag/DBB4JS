package SQL.queries;

import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;

import java.util.List;

public class Query18Parallel implements Query {

    // Note: parallel version of Query18SqlGroupByAndJoinMapMulti
    private final Query18SqlGroupByAndJoinMapMulti sequential = new Query18SqlGroupByAndJoinMapMulti();

    @Override
    public List<?> execute(TPCHStreamDB db) {
        return sequential.execute(db.parallel());
    }

    public static void main(String[] args) {
        new Query1Parallel().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }

}
