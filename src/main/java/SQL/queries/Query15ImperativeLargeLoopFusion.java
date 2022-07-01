package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.Query;
import SQL.dataset.SupplierRow;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;

import java.util.*;

public class Query15ImperativeLargeLoopFusion implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1996-01-01");
        Date max = new Date("1996-04-01");
        Map<Long, Query15Imperative.TempScan> tempScanMap = new HashMap<>();
        double maxRevenue = 0;
        long bigSuppkey = 0L;
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_shipdate().fromTo(min, max)) {
                long suppKey = l.l_suppkey();
                var value = tempScanMap.computeIfAbsent(suppKey, k -> new Query15Imperative.TempScan(suppKey));
                value.total_revenue += (l.l_extendedprice() * (1 - l.l_discount()));
                if (maxRevenue < value.total_revenue) {
                    maxRevenue = value.total_revenue;
                    bigSuppkey = suppKey;
                }
            }
        }

        HashMap<Long, Query15Imperative.Result> resultMap = new HashMap<>();
        for (SupplierRow s : db.supplier_arr()) {
            if (s.s_suppkey() == bigSuppkey) {
                double finalMaxRevenue = maxRevenue;
                resultMap
                        .computeIfAbsent(s.s_suppkey(), k -> new Query15Imperative.Result(s.s_suppkey(), s.s_name(), s.s_address(), s.s_phone(), finalMaxRevenue));
            }
        }
        Comparator<Query15Imperative.Result> resultComparator = Comparator.comparing(Query15Imperative.Result::s_suppkey);
        var arr = resultMap.values().toArray(Query15Imperative.Result[]::new);
        Arrays.sort(arr, resultComparator);
        return Arrays.asList(arr);
    }
}
