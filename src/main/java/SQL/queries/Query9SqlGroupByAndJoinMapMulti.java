package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.LongLongPair;
import SQL.sql_utils.tuple_types.StringIntPair;

import java.util.Comparator;
import java.util.List;

public class Query9SqlGroupByAndJoinMapMulti implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        var ns = SQLStreamUtils.streamJoinMapMulti(
                db.nation(),
                NationRow::n_nationkey,
                db.supplier(),
                SupplierRow::s_nationkey,
                (nation, supplier) -> new Query9.NationSupplier(nation.n_name(), supplier.s_suppkey())
        );


        var nationSupplierJoin = SQLStreamUtils.streamJoinMapMulti(
                ns,
                Query9.NationSupplier::s_suppkey,
                SQLStreamUtils.streamJoinMapMulti(
                        db.part().filter(row -> row.p_name().contains("green")),
                        PartRow::p_partkey,
                        db.partsupp(),
                        PartSuppRow::ps_partkey,
                        (part, partSupp) -> partSupp
                ),
                PartSuppRow::ps_suppkey,
                (nationSupplier, partSuppAndParJoin) -> new Query9.NationSupplierAndPartSupplier(
                        nationSupplier.n_name(),
                        partSuppAndParJoin.ps_suppkey(),
                        partSuppAndParJoin.ps_partkey(),
                        partSuppAndParJoin.ps_supplycost()
                )
        );
        var lineItemNewJoin = SQLStreamUtils.streamJoinMapMulti(
                nationSupplierJoin,
                (n) -> new LongLongPair(n.s_suppkey(), n.ps_partkey()),
                db.lineitem(),
                (l) -> new LongLongPair(l.l_suppkey(), l.l_partkey()),
                (n, l) -> new Query9.LineItemNew(n.n_name(), n.ps_supplycost(), l.l_orderkey(), l.l_extendedprice(), l.l_discount(), l.l_quantity())
        );

        Comparator<Query9Imperative.Result> c1 = Comparator.comparing(row -> row.nation);
        Comparator<Query9Imperative.Result> c2 = Comparator.comparing(row -> row.o_year);
        Comparator<Query9Imperative.Result> comparator = c1.thenComparing(c2.reversed());

//        Comparator<ProfitNoRecord> comparator = SQLStreamUtils.comparator(row->row.nation, row-> row.o_year);


        return SQLStreamUtils.streamJoinMapMulti(
                        lineItemNewJoin,
                        Query9.LineItemNew::l_orderkey,
                        db.orders(),
                        OrdersRow::o_orderkey,
                        (lineItemNew, order) -> new Query9.Profit(lineItemNew.n_name(),
                                order.o_orderdate().getYears(),
                                lineItemNew.l_extendedprice() * (1 - lineItemNew.l_discount())
                                        - lineItemNew.ps_supplycost() * lineItemNew.l_quantity()
                        )

                ).collect(SQLCollectors.groupingBy(profit ->
                                new StringIntPair(profit.nation(), profit.o_year()),
                        GroupByCollector.of(
                                (Query9.Profit p) -> new Query9Imperative.Result(p.nation(), p.o_year()),
                                (a, b) -> a.aggregate(b.amount()),
                                Query9Imperative.Result::combine
                        )
                ))
                .values()
                .stream()
                .sorted(comparator)
                .toList();
    }
}
