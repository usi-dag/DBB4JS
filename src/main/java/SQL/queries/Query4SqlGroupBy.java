package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.OrdersRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;

public class Query4SqlGroupBy implements Query {


    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1993-07-01");
        Date max = new Date("1993-10-01");
        Comparator<Query4Imperative.Result> comparator = Comparator.comparing(r -> r.o_orderpriority);
        // lineItemOrdersLeftSemiJoin
        return SQLStreamUtils.streamLeftSemiJoin(
                        db.orders().filter(l -> l.o_orderdate().fromTo(min, max)),
                        OrdersRow::o_orderkey,
                        db.lineitem().filter(l -> l.l_commitdate().compareTo(l.l_receiptdate()) < 0),
                        LineItemRow::l_orderkey
                ).collect(SQLCollectors.groupingBy(
                        OrdersRow::o_orderpriority,
                        GroupByCollector.of(
                                (OrdersRow o) -> new Query4Imperative.Result(o.o_orderpriority()),
                                (result, o1) -> result.aggregate(),
                                (a, b) -> null
                        )
                ))
                .values()
                .stream()
                .sorted(comparator)
                .toList();
    }

    public static void main(String[] args) {
        new Query4SqlGroupBy().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
