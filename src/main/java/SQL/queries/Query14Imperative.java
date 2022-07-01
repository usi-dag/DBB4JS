package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.PartRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;

import java.util.*;

public class Query14Imperative implements Query {

    record Result(double promo_revenue) {
        @Override
        public String toString() {
            return "Result{promo_revenue=" + promo_revenue + '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        var dateFrom = new Date("1995-09-01");
        var dateTo = new Date("1995-10-01");

        Map<Long, List<LineItemRow>> lineItemMap = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_shipdate().compareTo(dateFrom) >= 0 && l.l_shipdate().compareTo(dateTo) < 0) {
                var list = lineItemMap.computeIfAbsent(l.l_partkey(), k -> new ArrayList<>());
                list.add(l);
            }
        }
        double promoRevenue = 0;
        double totalRevenue = 0;
        for (PartRow p : db.part_arr()) {
            var res = lineItemMap.get(p.p_partkey());
            if (res != null) {
                var p_type = p.p_type().startsWith("PROMO");
                for (var l : res) {
                    var x = l.l_extendedprice() * (1 - l.l_discount());
                    if (p_type) {
                        promoRevenue += x;
                    }
                    totalRevenue += x;
                }
            }
        }

        double toRet = 100.D * promoRevenue / totalRevenue;
        return Collections.singletonList(new Result(toRet));
    }

    public static void main(String[] args) {
        new Query14Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }

}
