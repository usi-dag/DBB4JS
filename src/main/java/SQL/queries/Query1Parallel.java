package SQL.queries;

import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;

import java.util.List;

public class Query1Parallel implements Query {

    // Note: parallel version of Query1SqlGroupBy
    private final Query1SqlGroupBy sequential = new Query1SqlGroupBy();

    public List<?> execute(TPCHStreamDB db) {
        return sequential.execute(db.parallel());
    }

    public static void main(String[] args) {
        new Query1Parallel().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
