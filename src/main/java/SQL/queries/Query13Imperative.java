package SQL.queries;

import SQL.dataset.CustomerRow;
import SQL.dataset.OrdersRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.sql_utils.MarkedArrayList;
import SQL.sql_utils.StringMultiContains;

import java.util.*;

public class Query13Imperative implements Query {


    static class Result {
        final long c_count;
        int custdist;

        public Result(long c_count) {
            this.c_count = c_count;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "c_count=" + c_count +
                    ", custdist=" + custdist +
                    '}';
        }
    }


    static final class MutableLong {
        long val = 0;
    }


    @Override
    public List<?> execute(TPCHStreamDB db) {
        // build map (on left side -- it should be always built on left side)
        Map<Long, MarkedArrayList<CustomerRow>> custMap = new HashMap<>();
        for (CustomerRow c : db.customer_arr()) {
            custMap.computeIfAbsent(c.c_custkey(), k -> new MarkedArrayList<>()).add(c);
        }


        // scan the right side
        // note: we remove (instead of get) from the map
        //       doing so, we ensure that we emit a row on the left side only once

        // build (first part of) first groupby map
        Map<Long, MutableLong> countByCustomerKeyMap = new HashMap<>();
        StringMultiContains matcher = new StringMultiContains("special", "requests");
        for (OrdersRow o : db.orders_arr()) {
            if (!matcher.match(o.o_comment())) {
                MarkedArrayList<CustomerRow> joinMatch = custMap.get(o.o_custkey());
                if (joinMatch != null) {
                    joinMatch.mark();

                    for (CustomerRow c : joinMatch) {
                        Long custKey = c.c_custkey();
                        var count = countByCustomerKeyMap.computeIfAbsent(custKey, x -> new MutableLong());
                        count.val++;
                    }
                }
            }
        }

        // build (second part of) first groupby map
        //  here we iterate on the remaining pairs in the map - i.e., those without a matching order
        //  since there is no matching order, o_orderkey is always null - i.e., the c_count counter is 0
        for (MarkedArrayList<CustomerRow> notJoinedCustomerList : custMap.values()) {
            if (!notJoinedCustomerList.isMarked()) {
                for (CustomerRow customer : notJoinedCustomerList) {
                    countByCustomerKeyMap.computeIfAbsent(customer.c_custkey(), x -> new MutableLong());
                }
            }
        }

        // build second groupby map
        Map<Long, Result> resultMap = new HashMap<>();
        for (var c_count : countByCustomerKeyMap.values()) {
            var result = resultMap.computeIfAbsent(c_count.val, k -> new Result(c_count.val));
            result.custdist += 1;
        }

        Comparator<Result> c2 = Comparator.comparing(r -> -r.c_count);
        Comparator<Result> c1 = Comparator.comparing(r -> -r.custdist);
        Comparator<Result> comparator = c1.thenComparing(c2);

        var arr = resultMap.values().toArray(Result[]::new);
        Arrays.sort(arr, comparator);
        return Arrays.asList(arr);

    }

    public static void main(String[] args) {
        new Query13Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
