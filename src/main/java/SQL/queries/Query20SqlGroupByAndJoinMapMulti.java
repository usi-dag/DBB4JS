package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.LongLongPair;

import java.util.Comparator;
import java.util.List;

public class Query20SqlGroupByAndJoinMapMulti implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1994-01-01");
        Date max = new Date("1995-01-01");
        var lineItemJoin = db.lineitem()
                .filter(l -> l.l_shipdate().fromTo(min, max))
                .collect(SQLCollectors.groupingBy(l -> new LongLongPair(l.l_partkey(), l.l_suppkey()),
                        GroupByCollector.of(
                                (LineItemRow l) -> new Query20SqlGroupBy.LineItemJoin(l.l_partkey(), l.l_suppkey()),
                                Query20SqlGroupBy.LineItemJoin::aggregate,
                                (a, b) -> null
                        ))).values().stream();


        var partAndPartSuppJoin = SQLStreamUtils.streamRightSemiJoin(
                db.part().filter(p -> p.p_name().startsWith("forest")),
                PartRow::p_partkey,
                db.partsupp(),
                PartSuppRow::ps_partkey
        );


        var partSuppLineItemJoin = SQLStreamUtils.streamJoinMapMultiFilter(
                partAndPartSuppJoin,
                x -> new LongLongPair(x.ps_partkey(), x.ps_suppkey()),
                lineItemJoin,
                x -> new LongLongPair(x.l_partkey, x.l_suppkey),
                (pps, l) -> pps,
                (a, b) -> a.ps_availqty() > (b.finalizer().quantity)
        );


        var nationSupplierJoin = SQLStreamUtils.streamJoinMapMulti(
                db.nation().filter(n -> n.n_name().equals("CANADA")),
                NationRow::n_nationkey,
                db.supplier(),
                SupplierRow::s_nationkey,
                (n, s) -> s
        );

        Comparator<Query20SqlGroupBy.Result> comparator = Comparator.comparing(Query20SqlGroupBy.Result::s_name);

        return SQLStreamUtils.streamLeftSemiJoin(
                        nationSupplierJoin,
                        SupplierRow::s_suppkey,
                        partSuppLineItemJoin,
                        PartSuppRow::ps_suppkey
                )
                .map(x -> new Query20SqlGroupBy.Result(x.s_name(), x.s_address()))
                .sorted(comparator)
                .toList();
    }
}
