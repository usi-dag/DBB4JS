package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.queries.Query18Imperative.CustomerOrder;

import java.util.*;

public class Query18ImperativeLoopFusion implements Query {


    @Override
    public List<?> execute(TPCHStreamDB db) {
        // Filtering the orders that have a total quantity greater than 300.
        Map<Long, Query18Imperative.MutableDouble> lineItemSumMap = new HashMap<>();
        Set<Long> orderKeyList = new HashSet<>();
        var lineItemArr = db.lineitem_arr();
        for (LineItemRow l : lineItemArr) {
            Long orderKey = l.l_orderkey();
            Query18Imperative.MutableDouble d = lineItemSumMap.computeIfAbsent(orderKey, x -> new Query18Imperative.MutableDouble());
            d.val += l.l_quantity();
            if (d.val > 300) {
                orderKeyList.add(orderKey);
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


        Comparator<Query18Imperative.Result> c1 = Comparator.comparing(row -> -row.o_totalprice);
        Comparator<Query18Imperative.Result> c2 = Comparator.comparing(row -> row.o_orderdate);
        Comparator<Query18Imperative.Result> comparator = c1.thenComparing(c2);

        Map<CustomerOrder, Query18Imperative.Result> resultMap = new HashMap<>();
        for (LineItemRow l : lineItemArr) {
            var list = orderCustomerMap.get(l.l_orderkey());
            if (list != null) {
                var qty = l.l_quantity();
                for (var o : list) {
                    var get = resultMap.computeIfAbsent(o,
                            k -> new Query18Imperative.Result(o.c_name(), o.c_custkey(), o.o_orderkey(), o.o_orderdate(), o.o_totalprice()));
                    get.quantity += qty;
                }
            }

        }


        var arr = resultMap.values().toArray(Query18Imperative.Result[]::new);
        Arrays.sort(arr, comparator);
        if (arr.length > 100) {
            return Arrays.asList(arr).subList(0, 100);
        } else {
            return Arrays.asList(arr);
        }
    }

    public static void main(String[] args) {
        new Query18ImperativeLoopFusion().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
