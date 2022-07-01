package SQL.queries;

import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;

import java.util.List;

public class Query3Parallel implements Query {
    // note: parallel version of Query3SqlGroupByAndJoinMapMulti
    private final Query3SqlGroupByAndJoinMapMulti sequential = new Query3SqlGroupByAndJoinMapMulti();

    public List<?> execute(TPCHStreamDB db) {
        return sequential.execute(db.parallel());
    }

    public static void main(String[] args) {
        new Query1Parallel().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }

}
