package SQL.queries;

import SQL.dataset.OrdersRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query22SingleFilter implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Set<String> c_phone = Set.of("13", "31", "23", "29", "30", "18", "17");
        var customerGroupBy =
                db.customer().filter(c -> c.c_acctbal() > 0.D && c_phone.contains(c.c_phone().substring(0, 2)))
                        .collect(Collector.of(Query22.Acctbal::new, Query22.Acctbal::aggregate, (a, b) -> null, Query22.Acctbal::finalizer));


        var customersJoin =
                db.customer()
                        .filter(c -> c_phone.contains(c.c_phone().substring(0, 2)) && c.c_acctbal() > customerGroupBy.avg)
                        .map(c -> new Query22.InnerSelect(c.c_phone().substring(0, 2), c.c_acctbal(), c.c_custkey()));


        Comparator<Query22.Result> comparator = Comparator.comparing(r -> r.cntrycode);

        return SQLStreamUtils.streamLeftAntiJoin(
                        customersJoin,
                        Query22.InnerSelect::c_custkey,
                        db.orders(),
                        OrdersRow::o_custkey
                ).collect(
                        Collectors.groupingBy(Query22.InnerSelect::cntrycode, Collector.of(
                                Query22.Result::new, Query22.Result::aggregate, (a, b) -> null
                        ))
                )
                .values()
                .stream()
                .sorted(comparator)
                .toList();


    }
}
