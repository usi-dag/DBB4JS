package SQL.queries;

import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;

import java.util.Collections;
import java.util.List;

public class Query6SingleFilter implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date minShipDate = new Date("1994-01-01");
        Date maxShipDate = new Date("1995-01-01");
        var result = db.lineitem()
                .filter(row -> row.l_shipdate().compareTo(minShipDate) >= 0 && row.l_shipdate().compareTo(maxShipDate) < 0
                        && row.l_discount() >= 0.05 && row.l_discount() <= 0.07 && row.l_quantity() < 24)
                .mapToDouble(row -> row.l_extendedprice() * row.l_discount())
                .sum();
        return Collections.singletonList(new Query6.Result(result));
    }


}
