package SQL.queries;

import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import java.util.List;

public class Query6Parallel implements Query {

    // Note: parallel version of Query6SingleFilter
    private final Query6SingleFilter sequential = new Query6SingleFilter();

    @Override
    public List<?> execute(TPCHStreamDB db) {
        return sequential.execute(db.parallel());
    }

    public static void main(String[] args) {
        new Query1Parallel().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }

}
