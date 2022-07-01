package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.OrdersRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.sql_utils.MarkedArrayList;

import java.util.*;

public class Query4Imperative implements Query {

    static class Result {
        final String o_orderpriority;
        int order_count;

        public Result(String o_orderpriority) {
            this.o_orderpriority = o_orderpriority;
        }


        public void aggregate() {
            order_count += 1;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "o_orderpriority='" + o_orderpriority + '\'' +
                    ", order_count=" + order_count +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1993-07-01");
        Date max = new Date("1993-10-01");
        Map<Long, MarkedArrayList<OrdersRow>> markedMap = new HashMap<>();
        for (OrdersRow o : db.orders_arr()) {
            if (o.o_orderdate().fromTo(min, max)) {
                markedMap.computeIfAbsent(o.o_orderkey(), k -> new MarkedArrayList<>()).add(o);
            }
        }

        HashMap<String, Result> orderMap = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_commitdate().compareTo(l.l_receiptdate()) < 0) {
                var map = markedMap.get(l.l_orderkey());
                if (map != null && !map.isMarked()) {
                    map.mark();
                    for (OrdersRow o : map) {
                        orderMap.computeIfAbsent(o.o_orderpriority(), k -> new Result(o.o_orderpriority())).order_count += 1;
                    }
                }

            }
        }
        Comparator<Query4Imperative.Result> comparator = Comparator.comparing(r -> r.o_orderpriority);
        var arr = orderMap.values().toArray(Result[]::new);
        Arrays.sort(arr, comparator);
        return Arrays.asList(arr);
    }

    public static void main(String[] args) {
        new Query4Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
