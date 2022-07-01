package SQL.queries;

import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;

import java.util.List;

public class Query9Parallel implements Query {

    // Note: parallel version of Query9SqlGroupByAndJoinMapMulti
    private final Query9SqlGroupByAndJoinMapMulti sequential = new Query9SqlGroupByAndJoinMapMulti();

    @Override
    public List<?> execute(TPCHStreamDB db) {
        return sequential.execute(db.parallel());
    }

    public static void main(String[] args) {
        new Query1Parallel().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }

}
