package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query21 implements Query {

    record SupplierLineItem1Record(String s_name, long l_suppkey1, long l_orderkey1) {
    }

    static class Result {
        String s_name;
        int numwait;

        public void aggregate(SupplierLineItem1Record s) {
            s_name = s.s_name();
            numwait += 1;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "s_name='" + s_name + '\'' +
                    ", numwait=" + numwait +
                    '}';
        }
    }

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
                (s, l) -> new SupplierLineItem1Record(s.s_name(), l.l_suppkey(), l.l_orderkey())
        );


        var suppLineOrderJoin = SQLStreamUtils.streamJoin(
                supplierNationJoin,
                SupplierLineItem1Record::l_orderkey1,
                db.orders().filter(o -> o.o_orderstatus().equals("F")),
                OrdersRow::o_orderkey,
                (s, o) -> s
        );


        // left anti join
        var suppLineAndLineItem2Join = SQLStreamUtils.streamLeftAntiJoinFilter(
                suppLineOrderJoin,
                SupplierLineItem1Record::l_orderkey1,
                db.lineitem().filter(l -> l.l_receiptdate().compareTo(l.l_commitdate()) > 0),
                LineItemRow::l_orderkey,
                (l, r) -> l.l_suppkey1() != r.l_suppkey());

        // left semi join
        Comparator<Result> c1 = Comparator.comparing(r -> -r.numwait);
        Comparator<Result> c2 = Comparator.comparing(r -> r.s_name);
        Comparator<Result> comparator = c1.thenComparing(c2);

        return SQLStreamUtils.streamLeftSemiJoinFilter(
                        suppLineAndLineItem2Join,
                        SupplierLineItem1Record::l_orderkey1,
                        db.lineitem(),
                        LineItemRow::l_orderkey,
                        (l, r) -> l.l_suppkey1() != r.l_suppkey()
                )
                .collect(Collectors.groupingBy(SupplierLineItem1Record::s_name,
                        Collector.of(Result::new, Result::aggregate, (a, b) -> null)))
                .values()
                .stream()
                .sorted(comparator)
                .limit(100)
                .toList();
    }


    public static void main(String[] args) {
        new Query21().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
