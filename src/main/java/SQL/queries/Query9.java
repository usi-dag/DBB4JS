package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.LongLongPair;
import SQL.sql_utils.tuple_types.StringIntPair;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query9 implements Query {
    record Profit(String nation, int o_year, double amount) {
    }

    static class Result {
        String nation;
        int o_year;
        double amount;

        void accumulate(Profit other) {
            nation = other.nation;
            o_year = other.o_year;
            amount += other.amount;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nation='" + nation + '\'' +
                    ", o_year=" + o_year +
                    ", amount=" + amount +
                    '}';
        }
    }

    record NationSupplier(String n_name, long s_suppkey) {
    }

    record NationSupplierAndPartSupplier(String n_name, long s_suppkey, long ps_partkey, double ps_supplycost) {
    }

    record LineItemNew(String n_name, double ps_supplycost,
                       long l_orderkey, double l_extendedprice,
                       double l_discount, double l_quantity) {
    }


    public List<?> execute(TPCHStreamDB db) {

        var ns = SQLStreamUtils.streamJoin(
                db.nation(),
                NationRow::n_nationkey,
                db.supplier(),
                SupplierRow::s_nationkey,
                (nation, supplier) -> new NationSupplier(nation.n_name(), supplier.s_suppkey())
        );


        var nationSupplierJoin = SQLStreamUtils.streamJoin(
                ns,
                NationSupplier::s_suppkey,
                SQLStreamUtils.streamJoin(
                        db.part().filter(row -> row.p_name().contains("green")),
                        PartRow::p_partkey,
                        db.partsupp(),
                        PartSuppRow::ps_partkey,
                        (part, partSupp) -> partSupp
                ),
                PartSuppRow::ps_suppkey,
                (nationSupplier, partSuppAndParJoin) -> new NationSupplierAndPartSupplier(
                        nationSupplier.n_name(),
                        partSuppAndParJoin.ps_suppkey(),
                        partSuppAndParJoin.ps_partkey(),
                        partSuppAndParJoin.ps_supplycost()
                )
        );
        var lineItemNewJoin = SQLStreamUtils.streamJoin(
                nationSupplierJoin,
                (n) -> new LongLongPair(n.s_suppkey(), n.ps_partkey()),
                db.lineitem(),
                (l) -> new LongLongPair(l.l_suppkey(), l.l_partkey()),
                (n, l) -> new LineItemNew(n.n_name(), n.ps_supplycost(), l.l_orderkey(), l.l_extendedprice(), l.l_discount(), l.l_quantity())
        );

        Comparator<Result> c1 = Comparator.comparing(row -> row.nation);
        Comparator<Result> c2 = Comparator.comparing(row -> row.o_year);
        Comparator<Result> comparator = c1.thenComparing(c2.reversed());

//        Comparator<ProfitNoRecord> comparator = SQLStreamUtils.comparator(row->row.nation, row-> row.o_year);


        return SQLStreamUtils.streamJoin(
                        lineItemNewJoin,
                        LineItemNew::l_orderkey,
                        db.orders(),
                        OrdersRow::o_orderkey,
                        (lineItemNew, order) -> new Profit(lineItemNew.n_name(),
                                order.o_orderdate().getYears(),
                                lineItemNew.l_extendedprice() * (1 - lineItemNew.l_discount())
                                        - lineItemNew.ps_supplycost() * lineItemNew.l_quantity()
                        )

                ).collect(Collectors.groupingBy(profit ->
                                new StringIntPair(profit.nation(), profit.o_year()),
                        Collector.of(Result::new, Result::accumulate, (x, y) -> null)
                ))
                .values()
                .stream()
                .sorted(comparator)
                .toList();

    }

    public static void main(String[] args) {
        new Query9().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }

}
