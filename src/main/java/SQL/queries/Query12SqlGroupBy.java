package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.OrdersRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.StringStringPair;

import java.util.Comparator;
import java.util.List;

public class Query12SqlGroupBy implements Query {

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1994-01-01");
        Date max = new Date("1995-01-01");
        Comparator<Query12Imperative.Result> comparator = Comparator.comparing(r -> r.l_shipmode);
        return SQLStreamUtils
                .streamJoin(
                        db.lineitem()
                                .filter(l -> l.l_shipmode().equals("MAIL") || l.l_shipmode().equals("SHIP"))
                                .filter(l -> l.l_commitdate().compareTo(l.l_receiptdate()) < 0)
                                .filter(l -> l.l_shipdate().compareTo(l.l_commitdate()) < 0)
                                .filter(l -> l.l_receiptdate().fromTo(min, max))
                        ,
                        LineItemRow::l_orderkey,
                        db.orders(),
                        OrdersRow::o_orderkey,
                        (l, o) -> new StringStringPair(l.l_shipmode(), o.o_orderpriority())
                )
                .collect(SQLCollectors.groupingBy(
                        StringStringPair::fst,
                        GroupByCollector.of(
                                (StringStringPair r) -> new Query12Imperative.Result(r.fst()),
                                (a, b) -> a.aggregate(b.snd()),
                                (a, b) -> null
                        )
                ))
                .values()
                .stream()
                .sorted(comparator)
                .toList();
    }
}
