package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;

import java.util.*;

public class Query18Imperative implements Query {


    record CustomerOrder(String c_name, long c_custkey, long o_orderkey, Date o_orderdate, double o_totalprice) {
    }

    static class Result {
        final String c_name;
        final long c_custkey;
        final long o_orderkey;
        final Date o_orderdate;
        final double o_totalprice;
        double quantity = 0;

        public Result(String c_name, long c_custkey, long o_orderkey, Date o_orderdate, double o_totalprice) {
            this.c_name = c_name;
            this.c_custkey = c_custkey;
            this.o_orderkey = o_orderkey;
            this.o_orderdate = o_orderdate;
            this.o_totalprice = o_totalprice;
        }

        public Result combine(Result other) {
            this.quantity += other.quantity;
            return this;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "c_name='" + c_name + '\'' +
                    ", c_custkey=" + c_custkey +
                    ", o_orderkey=" + o_orderkey +
                    ", o_orderdate=" + o_orderdate +
                    ", o_totalprice=" + o_totalprice +
                    ", quantity=" + quantity +
                    '}';
        }
    }

    static final class MutableDouble {
        double val = 0;
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Map<Long, MutableDouble> lineItemSumMap = new HashMap<>();
        var lineItemArr = db.lineitem_arr();
        for (LineItemRow l : lineItemArr) {
            MutableDouble d = lineItemSumMap.computeIfAbsent(l.l_orderkey(), x -> new MutableDouble());
            d.val += l.l_quantity();
        }

        Set<Long> orderKeyList = new HashSet<>();
        for (var entry : lineItemSumMap.entrySet()) {
            if (entry.getValue().val > 300.D) {
                orderKeyList.add(entry.getKey());
            }
        }

        Map<Long, List<CustomerRow>> custOrderMap = new HashMap<>();
        for (CustomerRow c : db.customer_arr()) {
            var list = custOrderMap.computeIfAbsent(c.c_custkey(), k -> new ArrayList<>());
            list.add(c);
        }

        // join cust and (filtered) orders
        Map<Long, List<CustomerOrder>> orderCustomerMap = new HashMap<>();
        for (OrdersRow o : db.orders_arr()) {
            Long o_orderkey = o.o_orderkey();
            if (orderKeyList.contains(o_orderkey)) {
                var list = custOrderMap.get(o.o_custkey());
                if (list != null) {
                    Date o_orderdate = o.o_orderdate();
                    double o_totalprice = o.o_totalprice();
                    var get = orderCustomerMap.computeIfAbsent(o_orderkey, k -> new ArrayList<>());
                    for (CustomerRow c : list) {
                        get.add(new CustomerOrder(c.c_name(), c.c_custkey(), o.o_orderkey(), o_orderdate, o_totalprice));
                    }
                }
            }
        }


        Comparator<Result> c1 = Comparator.comparing(row -> -row.o_totalprice);
        Comparator<Result> c2 = Comparator.comparing(row -> row.o_orderdate);
        Comparator<Result> comparator = c1.thenComparing(c2);

        Map<CustomerOrder, Result> resultMap = new HashMap<>();
        for (LineItemRow l : lineItemArr) {
            var list = orderCustomerMap.get(l.l_orderkey());
            if (list != null) {
                var qty = l.l_quantity();
                for (var o : list) {
                    var get = resultMap.computeIfAbsent(o,
                            k -> new Result(o.c_name(), o.c_custkey(), o.o_orderkey(), o.o_orderdate(), o.o_totalprice()));
                    get.quantity += qty;
                }
            }

        }


        var arr = resultMap.values().toArray(Result[]::new);
        Arrays.sort(arr, comparator);
        if (arr.length > 100) {
            return Arrays.asList(arr).subList(0, 100);
        } else {
            return Arrays.asList(arr);
        }
    }


    public static void main(String[] args) {
        new Query18Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
