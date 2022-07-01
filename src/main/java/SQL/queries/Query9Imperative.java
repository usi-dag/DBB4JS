package SQL.queries;

import SQL.dataset.*;
import SQL.sql_utils.tuple_types.LongLongPair;
import SQL.sql_utils.tuple_types.StringIntPair;

import java.util.*;

public class Query9Imperative implements Query {


    record LineItemImp(String n_name, double ps_supplycost, double l_extendedprice,
                       double l_discount, double l_quantity) {
    }

    record NationPartSupplier(String n_name, double ps_supplycost) {
    }


    @Override
    public List<?> execute(TPCHStreamDB db) {
        // right side
        Map<Integer, List<NationRow>> mapNation = new HashMap<>();
        for (NationRow n : db.nation_arr()) {
            var list = mapNation.computeIfAbsent(n.n_nationkey(), k -> new ArrayList<>());
            list.add(n);
        }


        Map<Long, List<NationRow>> nationSupplierMap = new HashMap<>();
        for (SupplierRow s : db.supplier_arr()) {
            var listNation = mapNation.get(s.s_nationkey());
            if (listNation != null) {
                var get = nationSupplierMap.computeIfAbsent(s.s_suppkey(), k -> new ArrayList<>());
                get.addAll(listNation);
            }
        }

        // join between part and partSupp
        Map<Long, List<PartRow>> mapPartKey = new HashMap<>();
        for (PartRow p : db.part_arr()) {
            if (p.p_name().contains("green")) {
                var list = mapPartKey.computeIfAbsent(p.p_partkey(), k -> new ArrayList<>());
                list.add(p);
            }
        }
        // note: this materialization into partSuppRowList should be avoided
        Map<LongLongPair, List<NationPartSupplier>> HMap = new HashMap<>();

        for (PartSuppRow ps : db.partsupp_arr()) {
            var list = mapPartKey.get(ps.ps_partkey());
            if (list != null) {
                var list2 = nationSupplierMap.get(ps.ps_suppkey());
                if (list2 != null) {
                    var get = HMap.computeIfAbsent(new LongLongPair(ps.ps_suppkey(), ps.ps_partkey()), k -> new ArrayList<>());
                    for (var l : list2) {
                        get.add(new NationPartSupplier(l.n_name(), ps.ps_supplycost()));
                    }
                }
            }
        }


        HashMap<Long, List<LineItemImp>> lineItemNewHashMap = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            var list = HMap.get(new LongLongPair(l.l_suppkey(), l.l_partkey()));
            if (list != null) {
                var get = lineItemNewHashMap.computeIfAbsent(l.l_orderkey(), k -> new ArrayList<>());
                for (var n : list) {
                    get.add(new LineItemImp(n.n_name(), n.ps_supplycost(), l.l_extendedprice(), l.l_discount(), l.l_quantity()));
                }
            }
        }


        HashMap<StringIntPair, Result> lastMap = new HashMap<>();
        for (OrdersRow o : db.orders_arr()) {
            var res = lineItemNewHashMap.get(o.o_orderkey());
            if (res != null) {
                var o_year = o.o_orderdate().getYears();
                for (var l : res) {
                    var pair = new StringIntPair(l.n_name(), o_year);
                    var el = lastMap.computeIfAbsent(pair, k -> new Result(l.n_name(), o_year));
                    el.aggregate(l.l_extendedprice() * (1 - l.l_discount()) - l.ps_supplycost() * l.l_quantity());
                }
            }
        }

        var result = lastMap.values().toArray(Result[]::new);
        Comparator<Result> c1 = Comparator.comparing(row -> row.nation);
        Comparator<Result> c2 = Comparator.comparing(row -> row.o_year);
        Comparator<Result> comparator = c1.thenComparing(c2.reversed());
        Arrays.sort(result, comparator);
        return Arrays.asList(result);


    }

    static class Result {
        final String nation;
        final int o_year;
        double amount;

        public Result(String name, int o_year) {
            this.nation = name;
            this.o_year = o_year;
        }

        public void aggregate(double amount) {
            this.amount += amount;
        }

        public Result combine(Result other) {
            this.amount += other.amount;
            return this;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nation='" + nation + '\'' +
                    ", o_year=" + o_year +
                    ", amount=" + amount +
                    '}';
        }
    }

    public static void main(String[] args) {
        new Query9Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
