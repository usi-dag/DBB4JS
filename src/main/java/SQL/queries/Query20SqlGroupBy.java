package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.LongLongPair;

import java.util.Comparator;
import java.util.List;

public class Query20SqlGroupBy implements Query {


    static class LineItemJoin {
        final long l_partkey;
        final long l_suppkey;
        double quantity;

        public LineItemJoin(long l_partkey, long l_suppkey) {
            this.l_partkey = l_partkey;
            this.l_suppkey = l_suppkey;
        }

        public void aggregate(LineItemRow l) {
            quantity += l.l_quantity();
        }

        public LineItemJoin finalizer() {
            quantity = quantity * 0.5;
            return this;
        }
    }

    record Result(String s_name, String s_address) {
        @Override
        public String toString() {
            return "Result{" +
                    "s_name='" + s_name + '\'' +
                    ", s_address='" + s_address + '\'' +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1994-01-01");
        Date max = new Date("1995-01-01");
        var lineItemJoin = db.lineitem()
                .filter(l -> l.l_shipdate().fromTo(min, max))
                .collect(SQLCollectors.groupingBy(l -> new LongLongPair(l.l_partkey(), l.l_suppkey()),
                        GroupByCollector.of(
                                (LineItemRow l) -> new LineItemJoin(l.l_partkey(), l.l_suppkey()),
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

        Comparator<Result> comparator = Comparator.comparing(Result::s_name);

        return SQLStreamUtils.streamLeftSemiJoin(
                        nationSupplierJoin,
                        SupplierRow::s_suppkey,
                        partSuppLineItemJoin,
                        PartSuppRow::ps_suppkey
                )
                .map(x -> new Result(x.s_name(), x.s_address()))
                .sorted(comparator)
                .toList();

    }

    public static void main(String[] args) {
        new Query20SqlGroupBy().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
