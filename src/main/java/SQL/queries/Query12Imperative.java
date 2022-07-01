package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.OrdersRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;

import java.util.*;

public class Query12Imperative implements Query {

    static class Result {
        final String l_shipmode;
        double high_line_count;
        double low_line_count;

        public Result(String l_shipmode) {
            this.l_shipmode = l_shipmode;
        }

        public void aggregate(String o_orderpriority) {
            if (o_orderpriority.equals("1-URGENT") || o_orderpriority.equals("2-HIGH")) {
                high_line_count += 1;
            } else {
                low_line_count += 1;
            }
        }

        @Override
        public String toString() {
            return "Result{" +
                    "l_shipmode='" + l_shipmode + '\'' +
                    ", high_line_count=" + high_line_count +
                    ", low_line_count=" + low_line_count +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1994-01-01");
        Date max = new Date("1995-01-01");

        Map<Long, List<LineItemRow>> lineItemMap = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_shipmode().equals("MAIL") || l.l_shipmode().equals("SHIP")) {
                if (l.l_commitdate().compareTo(l.l_receiptdate()) < 0 & l.l_shipdate().compareTo(l.l_commitdate()) < 0 & l.l_receiptdate().fromTo(min, max)) {
                    var value = lineItemMap.computeIfAbsent(l.l_orderkey(), k -> new ArrayList<>());
                    value.add(l);
                }
            }

        }
        Map<String, Result> resMap = new HashMap<>();
        for (OrdersRow o : db.orders_arr()) {
            var list = lineItemMap.get(o.o_orderkey());
            if (list != null) {
                int high_line_count = 0;
                int low_line_count = 0;
                if ((o.o_orderpriority().equals("1-URGENT") || o.o_orderpriority().equals("2-HIGH"))) {
                    high_line_count = 1;
                } else {
                    low_line_count = 1;
                }
                for (var l : list) {
                    var result = resMap.computeIfAbsent(l.l_shipmode(), k -> new Result(l.l_shipmode()));
                    result.high_line_count += high_line_count;
                    result.low_line_count += low_line_count;
                }
            }
        }
        var arr = resMap.values().toArray(Result[]::new);
        Arrays.sort(arr, Comparator.comparing(result -> result.l_shipmode));
        return Arrays.asList(arr);
    }

    public static void main(String[] args) {
        new Query12Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
