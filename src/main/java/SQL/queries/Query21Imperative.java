package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.MarkedRowWrapper;

import java.util.*;

public class Query21Imperative implements Query {

    static class Result {
        final String s_name;
        int numwait = 0;

        public Result(String s_name) {
            this.s_name = s_name;
        }


        public void aggregate() {
            numwait++;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "s_name='" + s_name + '\'' +
                    ", numwait=" + numwait +
                    '}';
        }
    }


    @Override
    public List<?> execute(TPCHStreamDB db) {
        Map<Integer, List<NationRow>> nationMap = new HashMap<>();
        for (NationRow n : db.nation_arr()) {
            if (n.n_name().equals("SAUDI ARABIA")) {
                var list = nationMap.computeIfAbsent(n.n_nationkey(), k -> new ArrayList<>());
                list.add(n);
            }
        }


        Map<Long, List<SupplierRow>> suppMap = new HashMap<>();
        for (SupplierRow s : db.supplier_arr()) {
            var get = nationMap.get(s.s_nationkey());
            if (get != null) {
                var list = suppMap.computeIfAbsent(s.s_suppkey(), k -> new ArrayList<>());
                list.add(s);
            }

        }


        Map<Long, List<Query21.SupplierLineItem1Record>> supplierLineItem1Map = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_receiptdate().compareTo(l.l_commitdate()) > 0) {
                var get = suppMap.get(l.l_suppkey());
                if (get != null) {
                    var list = supplierLineItem1Map.computeIfAbsent(l.l_orderkey(), k -> new ArrayList<>());
                    for (SupplierRow s : get) {
                        list.add(new Query21.SupplierLineItem1Record(s.s_name(), l.l_suppkey(), l.l_orderkey()));
                    }
                }
            }
        }

        Map<Long, List<MarkedRowWrapper<Query21.SupplierLineItem1Record>>> markedMap = new HashMap<>();
        for (OrdersRow o : db.orders_arr()) {
            if (o.o_orderstatus().equals("F")) {
                Long orderKey = o.o_orderkey();
                var get = supplierLineItem1Map.get(orderKey);
                if (get != null) {
                    for (var el : get) {
                        markedMap.computeIfAbsent(orderKey, k -> new ArrayList<>()).add(new MarkedRowWrapper<>(el));
                    }
                }
            }
        }
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_receiptdate().compareTo(l.l_commitdate()) > 0) {
                var list = markedMap.get(l.l_orderkey());
                if (list != null) {
                    for (var slr : list) {
                        if (slr.element().l_suppkey1() != l.l_suppkey()) {
                            slr.mark();
                        }
                    }
                }
            }
        }
        Map<Long, List<MarkedRowWrapper<Query21.SupplierLineItem1Record>>> leftAntiJoinMap = new HashMap<>();
        var t = markedMap.entrySet();
        for (var slr : t) {
            for (var element : slr.getValue()) {
                if (!element.isMarked()) {
                    leftAntiJoinMap.computeIfAbsent(element.element().l_orderkey1(), k -> new ArrayList<>()).add(element);
                }
            }
        }

        Map<String, Query21Imperative.Result> lastMap = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            var list = leftAntiJoinMap.get(l.l_orderkey());
            if (list != null) {
                for (var slr : list) {
                    var element = slr.element();
                    if (element.l_suppkey1() != l.l_suppkey() && !slr.isMarked()) {
                        lastMap.computeIfAbsent(element.s_name(), k -> new Query21Imperative.Result(element.s_name())).numwait += 1;
                        slr.mark();
                    }
                }
            }
        }


        Comparator<Query21Imperative.Result> c1 = Comparator.comparing(r -> -r.numwait);
        Comparator<Query21Imperative.Result> c2 = Comparator.comparing(r -> r.s_name);
        Comparator<Query21Imperative.Result> comparator = c1.thenComparing(c2);

        var arr = lastMap.values().toArray(Query21Imperative.Result[]::new);
        Arrays.sort(arr, comparator);
        if (arr.length < 100) {
            return Arrays.asList(arr);
        }
        return Arrays.asList(arr).subList(0, 100);

    }

    public static void main(String[] args) {
        new Query21Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
