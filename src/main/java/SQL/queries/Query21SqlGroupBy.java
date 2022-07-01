package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;

public class Query21SqlGroupBy implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        var nationSupplierJoin = SQLStreamUtils.streamJoin(
                db.nation().filter(n -> n.n_name().equals("SAUDI ARABIA")),
                NationRow::n_nationkey,
                db.supplier(),
                SupplierRow::s_nationkey,
                (n, s) -> s
        );


        var supplierNationJoin = SQLStreamUtils.streamJoin(
                nationSupplierJoin,
                SupplierRow::s_suppkey,
                db.lineitem().filter(l -> l.l_receiptdate().compareTo(l.l_commitdate()) > 0),
                LineItemRow::l_suppkey,
                (s, l) -> new Query21.SupplierLineItem1Record(s.s_name(), l.l_suppkey(), l.l_orderkey())
        );


        var suppLineOrderJoin = SQLStreamUtils.streamJoin(
                supplierNationJoin,
                Query21.SupplierLineItem1Record::l_orderkey1,
                db.orders().filter(o -> o.o_orderstatus().equals("F")),
                OrdersRow::o_orderkey,
                (s, o) -> s
        );


        // left anti join
        var suppLineAndLineItem2Join = SQLStreamUtils.streamLeftAntiJoinFilter(
                suppLineOrderJoin,
                Query21.SupplierLineItem1Record::l_orderkey1,
                db.lineitem().filter(l -> l.l_receiptdate().compareTo(l.l_commitdate()) > 0),
                LineItemRow::l_orderkey,
                (l, r) -> l.l_suppkey1() != r.l_suppkey());

        // left semi join
        Comparator<Query21Imperative.Result> c1 = Comparator.comparing(r -> -r.numwait);
        Comparator<Query21Imperative.Result> c2 = Comparator.comparing(r -> r.s_name);
        Comparator<Query21Imperative.Result> comparator = c1.thenComparing(c2);

        return SQLStreamUtils.streamLeftSemiJoinFilter(
                        suppLineAndLineItem2Join,
                        Query21.SupplierLineItem1Record::l_orderkey1,
                        db.lineitem(),
                        LineItemRow::l_orderkey,
                        (l, r) -> l.l_suppkey1() != r.l_suppkey()
                )
                .collect(SQLCollectors.groupingBy(Query21.SupplierLineItem1Record::s_name,
                        GroupByCollector.of(
                                (Query21.SupplierLineItem1Record s) -> new Query21Imperative.Result(s.s_name()),
                                (a, b) -> a.aggregate(),
                                (a, b) -> null
                        )))
                .values()
                .stream()
                .sorted(comparator)
                .limit(100)
                .toList();
    }
}
