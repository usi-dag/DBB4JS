package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;

import java.util.*;

public class Query3Imperative implements Query {

    static class Result {
        final long l_orderkey;
        double revenue;
        final Date o_orderdate;
        final int o_shippriority;

        public Result(long l_orderkey, Date o_orderdate, int o_shippriority) {
            this.l_orderkey = l_orderkey;
            this.o_orderdate = o_orderdate;
            this.o_shippriority = o_shippriority;
        }

        public void aggregate(double l_extendedprice, double l_discount) {
            revenue += l_extendedprice * (1 - l_discount);
        }

        public Result combine(Result other) {
            revenue += other.revenue;
            return this;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "l_orderkey=" + l_orderkey +
                    ", revenue=" + revenue +
                    ", o_orderdate=" + o_orderdate +
                    ", o_shippriority=" + o_shippriority +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date o_orderdateMax = new Date("1995-03-15");
        Date l_shipdateMin = new Date("1995-03-15");

        // simple map
        Map<Long, List<CustomerRow>> custMap = new HashMap<>();
        var customers = db.customer_arr();
        for (var c : customers) {
            if (c.c_mktsegment().equals("BUILDING")) {
                var list = custMap.computeIfAbsent(c.c_custkey(), k -> new ArrayList<>());
                list.add(c);
            }
        }

        var orders = db.orders_arr();
        // map orderkey list of ordersrow
        HashMap<Long, List<OrdersRow>> map = new HashMap<>();
        for (OrdersRow o : orders) {
            if (o.o_orderdate().compareTo(o_orderdateMax) < 0) {
                var list = custMap.get(o.o_custkey());
                if (list != null) {
                    var curr = map.computeIfAbsent(o.o_orderkey(), k -> new ArrayList<>());
                    curr.add(o);
                }
            }
        }

        HashMap<Query3.Q3Key, Result> finalMap = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_shipdate().compareTo(l_shipdateMin) > 0) {
                var listOrders = map.get(l.l_orderkey());
                if (listOrders != null) {
                    var l_orderkey = l.l_orderkey();
                    for (OrdersRow o : listOrders) {
                        var curr = finalMap
                                .computeIfAbsent(new Query3.Q3Key(l_orderkey, o.o_orderdate(), o.o_shippriority()),
                                        k -> new Result(l_orderkey, o.o_orderdate(), o.o_shippriority()));
                        curr.revenue += l.l_extendedprice() * (1 - l.l_discount());
                    }
                }
            }
        }

        Comparator<Result> c1 = Comparator.comparing(row -> -row.revenue);
        Comparator<Result> c2 = Comparator.comparing(row -> row.o_orderdate);
        Comparator<Result> comparator = c1.thenComparing(c2);

        var arr = finalMap.values().toArray(Result[]::new);
        Arrays.sort(arr, comparator);
        if (arr.length < 10) {
            return Arrays.asList(arr);
        }
        return Arrays.asList(arr).subList(0, 10);
    }

    public static void main(String[] args) {
        new Query3Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
