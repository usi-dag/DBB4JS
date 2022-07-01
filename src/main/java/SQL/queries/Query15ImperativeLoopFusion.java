package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.Query;
import SQL.dataset.SupplierRow;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;

import java.util.*;

public class Query15ImperativeLoopFusion implements Query {

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1996-01-01");
        Date max = new Date("1996-04-01");
        Map<Long, Query15Imperative.TempScan> tempScanMap = new HashMap<>();
        double maxRevenue =  0;
        for(LineItemRow l : db.lineitem_arr()){
            if(l.l_shipdate().fromTo(min, max)) {
                var value = tempScanMap.computeIfAbsent(l.l_suppkey(), k -> new Query15Imperative.TempScan(l.l_suppkey()));
                value.total_revenue += (l.l_extendedprice() * (1- l.l_discount()));
                if (maxRevenue < value.total_revenue){
                    maxRevenue = value.total_revenue;
                }
            }
        }

        HashMap<Long, Double> tempScanMap2 = new HashMap<>();
        for(var t : tempScanMap.values()){
            if(t.total_revenue == maxRevenue){
                tempScanMap2.putIfAbsent(t.supplier_no, t.total_revenue);
            }
        }

        HashMap<Long, Query15Imperative.Result> resultMap = new HashMap<>();
        for(SupplierRow s : db.supplier_arr()){
            Long suppKey = s.s_suppkey();
            var list = tempScanMap2.get(suppKey);
            if(list != null){
                resultMap
                        .computeIfAbsent(suppKey,k -> new Query15Imperative.Result(s.s_suppkey(), s.s_name(),s.s_address(), s.s_phone(), list) );
            }
        }
        Comparator<Query15Imperative.Result> resultComparator = Comparator.comparing(Query15Imperative.Result::s_suppkey);
        var arr = resultMap.values().toArray(Query15Imperative.Result[]::new);
        Arrays.sort(arr, resultComparator);
        return Arrays.asList(arr);

    }
}
