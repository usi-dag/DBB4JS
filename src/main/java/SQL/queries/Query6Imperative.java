package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;

import java.util.Collections;
import java.util.List;

public class Query6Imperative implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        final Date minShipDate = new Date("1994-01-01");
        final Date maxShipDate = new Date("1995-01-01");
        double result = 0;
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_shipdate().compareTo(minShipDate) >= 0 && l.l_shipdate().compareTo(maxShipDate) < 0 && l.l_discount() >= 0.05 && l.l_discount() <= 0.07 && l.l_quantity() < 24) {
                result += l.l_extendedprice() * l.l_discount();
            }
        }
        return Collections.singletonList(new Query6.Result(result));
    }

    public static void main(String[] args) {
        new Query6Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }

}
