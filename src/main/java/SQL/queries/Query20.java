package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.LongLongPair;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query20 implements Query {

    static class LineItemJoin {
        long l_partkey;
        long l_suppkey;
        double quantity;


        public void aggregate(LineItemRow l) {
            l_partkey = l.l_partkey();
            l_suppkey = l.l_suppkey();
            quantity += l.l_quantity();
        }

        public LineItemJoin finalizer() {
            quantity = quantity * 0.5;
            return this;
        }
    }


    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1994-01-01");
        Date max = new Date("1995-01-01");
        var lineItemJoin = db.lineitem()
                .filter(l -> l.l_shipdate().fromTo(min, max))
                .collect(Collectors.groupingBy(l -> new LongLongPair(l.l_partkey(), l.l_suppkey()),
                        Collector.of(
                                LineItemJoin::new,
                                LineItemJoin::aggregate,
                                (a, b) -> null
                        ))).values().stream();


        var partAndPartSuppJoin = SQLStreamUtils.streamRightSemiJoin(
                db.part().filter(p -> p.p_name().startsWith("forest")),
                PartRow::p_partkey,
                db.partsupp(),
                PartSuppRow::ps_partkey
        );


        var partSuppLineItemJoin = SQLStreamUtils.streamJoinFilter(
                partAndPartSuppJoin,
                x -> new LongLongPair(x.ps_partkey(), x.ps_suppkey()),
                lineItemJoin,
                x -> new LongLongPair(x.l_partkey, x.l_suppkey),
                (pps, l) -> pps,
                (a, b) -> a.ps_availqty() > (b.finalizer().quantity)
        );


        var nationSupplierJoin = SQLStreamUtils.streamJoin(
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
