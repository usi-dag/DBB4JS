package SQL.queries;

import SQL.dataset.CustomerRow;
import SQL.dataset.OrdersRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.sql_utils.MarkedRowWrapper;

import java.util.*;

public class Query22Imperative implements Query {

    static class Result {
        final String cntrycode;
        int numcust;
        double totacctbal;

        public Result(String cntrycode) {
            this.cntrycode = cntrycode;
        }

        public void aggregate(double acctbal) {
            totacctbal += acctbal;
            numcust += 1;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "cntrycode='" + cntrycode + '\'' +
                    ", numcust=" + numcust +
                    ", totacctbal=" + String.format("%.2f", totacctbal) +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Set<String> c_phone = Set.of("13", "31", "23", "29", "30", "18", "17");
        double total = 0;
        double n = 0;
        for (CustomerRow c : db.customer_arr()) {
            if (c_phone.contains(c.c_phone().substring(0, 2)) && c.c_acctbal() > 0.D) {
                total += c.c_acctbal();
                n++;
            }
        }
        total = total / n;


        Map<Long, List<MarkedRowWrapper<Query22.InnerSelect>>> leftAntiJoinMap = new HashMap<>();
        for (CustomerRow c : db.customer_arr()) {
            if (c_phone.contains(c.c_phone().substring(0, 2)) && c.c_acctbal() > total) {
                leftAntiJoinMap.computeIfAbsent(c.c_custkey(), k -> new ArrayList<>())
                        .add(new MarkedRowWrapper<>(new Query22.InnerSelect(c.c_phone().substring(0, 2), c.c_acctbal(), c.c_custkey())));
            }
        }
        Map<String, Result> resultMap = new HashMap<>();
        for (OrdersRow o : db.orders_arr()) {
            var lst = leftAntiJoinMap.get(o.o_custkey());
            if (lst != null) {
                for (var row : lst) {
                    row.mark();
                }
            }
        }
        for (var entry : leftAntiJoinMap.entrySet()) {
            for (var element : entry.getValue()) {
                if (!element.isMarked()) {
                    var is = element.element();
                    var result = resultMap.computeIfAbsent(is.cntrycode(), k -> new Result(is.cntrycode()));
                    result.aggregate(is.c_acctbal());
                }
            }
        }

        Comparator<Result> comparator = Comparator.comparing(r -> r.cntrycode);

        var arr = resultMap.values().toArray(Result[]::new);
        Arrays.sort(arr, comparator);
        return Arrays.asList(arr);

    }

    public static void main(String[] args) {
        new Query22Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
