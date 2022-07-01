package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.Query;
import SQL.dataset.SupplierRow;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query15 implements Query {

    record Result(
            long s_suppkey,
            String s_name,
            String s_address,
            String s_phone,
            double total_revenue


    ) {
        @Override
        public String toString() {
            return "Result{" +
                    "s_suppkey=" + s_suppkey +
                    ", s_name='" + s_name + '\'' +
                    ", s_address='" + s_address + '\'' +
                    ", s_phone='" + s_phone + '\'' +
                    ", total_revenue=" + total_revenue +
                    '}';
        }
    }


    static class TempScan {
        long supplier_no;
        double total_revenue;

        public void aggregate(LineItemRow l) {
            supplier_no = l.l_suppkey();
            total_revenue += l.l_extendedprice() * (1.d - l.l_discount());
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1996-01-01");
        Date max = new Date("1996-04-01");


        var temScanList = db.lineitem().filter(l -> l.l_shipdate().fromTo(min, max))
                .collect(Collectors.groupingBy(LineItemRow::l_suppkey, Collector.of(
                        TempScan::new,
                        TempScan::aggregate,
                        (a, b) -> null
                ))).values().stream().toList();

        Comparator<TempScan> cMax = Comparator.comparing(t -> t.total_revenue);
        var firstJoin = SQLStreamUtils.streamJoin(
                temScanList.stream().max(cMax).stream(),
                t -> t.total_revenue,
                temScanList.stream(),
                t -> t.total_revenue,
                (t1, t2) -> t2
        );

        Comparator<Result> comparator = Comparator.comparing(r -> r.s_suppkey);

        return SQLStreamUtils.streamJoin(
                        firstJoin,
                        t -> t.supplier_no,
                        db.supplier(),
                        SupplierRow::s_suppkey,
                        (t, s) -> new Result(s.s_suppkey(), s.s_name(), s.s_address(), s.s_phone(), t.total_revenue)
                )
                .sorted(comparator)
                .toList();

    }

    public static void main(String[] args) {
        new Query15().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
