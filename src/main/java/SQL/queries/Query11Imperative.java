package SQL.queries;

import SQL.dataset.*;

import java.util.*;

public class Query11Imperative implements Query {

    static class Result {
        final long ps_partkey;
        double value;

        public Result(long p_partkey) {
            this.ps_partkey = p_partkey;
        }


        public void aggregate(double ps_supplycost, double ps_availqty) {
            value += ps_supplycost * ps_availqty;

        }

        @Override
        public String toString() {
            return "Result{" +
                    "ps_partkey=" + ps_partkey +
                    ", value=" + value +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        ///////////  innerNationSupplierJoin
        Map<Integer, List<NationRow>> map = new HashMap<>();
        for (var n : db.nation_arr()) {
            if (n.n_name().equals("GERMANY")) {
                var value = map.computeIfAbsent(n.n_nationkey(), k -> new ArrayList<>());
                value.add(n);
            }
        }


        Map<Long, List<SupplierRow>> supplierMap = new HashMap<>();
        for (var s : db.supplier_arr()) {
            var list = map.get(s.s_nationkey());
            if (list != null) {
                supplierMap.computeIfAbsent(s.s_suppkey(), k -> new ArrayList<>()).add(s);
            }

        }
        double sum = 0;
        for (var ps : db.partsupp_arr()) {
            var list = supplierMap.get(ps.ps_suppkey());
            if (list != null) {
                sum += ps.ps_supplycost() * ps.ps_availqty();
            }
        }
        sum = sum * 0.0001;


        Map<Integer, List<NationRow>> map2 = new HashMap<>();
        for (var n : db.nation_arr()) {
            if (n.n_name().equals("GERMANY")) {
                var value = map2.computeIfAbsent(n.n_nationkey(), k -> new ArrayList<>());
                value.add(n);
            }
        }


        Map<Long, List<SupplierRow>> supplierMap2 = new HashMap<>();
        for (var s : db.supplier_arr()) {
            var list = map2.get(s.s_nationkey());
            if (list != null) {
                supplierMap2.computeIfAbsent(s.s_suppkey(), k -> new ArrayList<>()).add(s);
            }

        }
        Map<Long, Result> partSuppRowHashMap = new HashMap<>();
        for (var ps : db.partsupp_arr()) {
            var list = supplierMap2.get(ps.ps_suppkey());
            if (list != null) {
                var result = partSuppRowHashMap.computeIfAbsent(ps.ps_partkey(), k -> new Result(ps.ps_partkey()));
                result.value += (ps.ps_supplycost() * ps.ps_availqty());
            }
        }


        Comparator<Result> comparator = Comparator.comparingDouble(o -> -o.value);
        List<Result> resultList = new ArrayList<>();
        for (var result : partSuppRowHashMap.values()) {
            if (result.value > sum) {
                resultList.add(result);
            }
        }
        resultList.sort(comparator);
        return resultList;

    }

    public static void main(String[] args) {
        new Query11Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
