package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.Query;
import SQL.dataset.SupplierRow;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;

import java.util.*;

public class Query15Imperative implements Query {

    static class TempScan {
        final long supplier_no;
        double total_revenue;

        public TempScan(long supplier_no) {
            this.supplier_no = supplier_no;
        }

        public void aggregate(double l_extendeprice, double l_discount) {
            total_revenue += l_extendeprice * (1 - l_discount);
        }
    }

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

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1996-01-01");
        Date max = new Date("1996-04-01");
        Map<Long, TempScan> tempScanMap = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_shipdate().fromTo(min, max)) {
                var value = tempScanMap.computeIfAbsent(l.l_suppkey(), k -> new TempScan(l.l_suppkey()));
                value.total_revenue += (l.l_extendedprice() * (1 - l.l_discount()));
            }
        }

        double maxRevenue = 0;
        for (var t : tempScanMap.values()) {
            if (t.total_revenue > maxRevenue) {
                maxRevenue = t.total_revenue;
            }
        }

        HashMap<Long, Double> tempScanMap2 = new HashMap<>();
        for (var t : tempScanMap.values()) {
            if (t.total_revenue == maxRevenue) {
                tempScanMap2.putIfAbsent(t.supplier_no, t.total_revenue);
            }
        }

        HashMap<Long, Result> resultMap = new HashMap<>();
        for (SupplierRow s : db.supplier_arr()) {
            Long suppKey = s.s_suppkey();
            var list = tempScanMap2.get(suppKey);
            if (list != null) {
                resultMap
                        .computeIfAbsent(suppKey, k -> new Result(s.s_suppkey(), s.s_name(), s.s_address(), s.s_phone(), list));
            }
        }
        Comparator<Result> resultComparator = Comparator.comparing(r -> r.s_suppkey);
        var arr = resultMap.values().toArray(Result[]::new);
        Arrays.sort(arr, resultComparator);
        return Arrays.asList(arr);

    }

    public static void main(String[] args) {
        new Query15Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
