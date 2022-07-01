package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.tuple_types.LongIntPair;

import java.util.*;

public class Query7Imperative implements Query {

    record NationsJoined(String n1_name, String n2_name, int n1_nationkey) {
    }

    static class Result {
        final String supp_nation;
        final String cust_nation;
        final int l_year;
        double revenue;

        public Result(String supp_nation, String cust_nation, int l_year) {
            this.supp_nation = supp_nation;
            this.cust_nation = cust_nation;
            this.l_year = l_year;
        }

        public void aggregate(double l_extendedprice, double l_discount) {
            revenue += l_extendedprice * (1 - l_discount);
        }

        @Override
        public String toString() {
            return "Result{" +
                    "supp_nation='" + supp_nation + '\'' +
                    ", cust_nation='" + cust_nation + '\'' +
                    ", l_year=" + l_year +
                    ", revenue=" + revenue +
                    '}';
        }
    }


    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1995-01-01");
        Date max = new Date("1996-12-31");


        Map<Integer, List<NationsJoined>> nationMap = new HashMap<>();
        var nationArr = db.nation_arr();
        for (NationRow row : nationArr) {
            for (NationRow nationRow : nationArr) {
                if ((row.n_name().equals("GERMANY") && nationRow.n_name().equals("FRANCE")) ||
                        (row.n_name().equals("FRANCE") && nationRow.n_name().equals("GERMANY"))) {
                    var list = nationMap.computeIfAbsent(nationRow.n_nationkey(), k -> new ArrayList<>());
                    list.add(new NationsJoined(row.n_name(), nationRow.n_name(), row.n_nationkey()));
                }
            }

        }

        Map<Long, List<NationsJoined>> custNationMap = new HashMap<>();
        for (CustomerRow c : db.customer_arr()) {
            var list = nationMap.get(c.c_nationkey());
            if (list != null) {
                var get = custNationMap.computeIfAbsent(c.c_custkey(), k -> new ArrayList<>());
                get.addAll(list);
            }
        }


        Map<Long, List<NationsJoined>> custOrderNationMap = new HashMap<>();
        for (OrdersRow o : db.orders_arr()) {
            var list = custNationMap.get(o.o_custkey());
            if (list != null) {
                var get = custOrderNationMap.computeIfAbsent(o.o_orderkey(), k -> new ArrayList<>());
                get.addAll(list);
            }
        }

        Map<LongIntPair, List<SupplierRow>> supplierMap = new HashMap<>();
        for (SupplierRow s : db.supplier_arr()) {
            supplierMap.computeIfAbsent(new LongIntPair(s.s_suppkey(), s.s_nationkey()), k -> new ArrayList<>()).add(s);
        }

        Map<Query7.GroupBy, Result> lastMap = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_shipdate().compareTo(min) >= 0 && l.l_shipdate().compareTo(max) <= 0) {
                var list = custOrderNationMap.get(l.l_orderkey());
                if (list != null) {
                    for (NationsJoined n : list) {
                        var list2 = supplierMap.get(new LongIntPair(l.l_suppkey(), n.n1_nationkey()));
                        if (list2 != null) {
                            var get = lastMap
                                    .computeIfAbsent(new Query7.GroupBy(n.n1_name(), n.n2_name(), l.l_shipdate().getYears()),
                                            k -> new Result(n.n1_name(), n.n2_name(), l.l_shipdate().getYears()));
                            get.revenue += l.l_extendedprice() * (1 - l.l_discount());
                        }
                    }
                }
            }
        }


        Comparator<Result> c1 = Comparator.comparing(r -> r.supp_nation);
        Comparator<Result> c2 = Comparator.comparing(r -> r.cust_nation);
        Comparator<Result> c3 = Comparator.comparing(r -> r.l_year);
        Comparator<Result> comparator = c1.thenComparing(c2).thenComparing(c3);

        var arr = lastMap.values().toArray(Result[]::new);
        Arrays.sort(arr, comparator);
        return Arrays.asList(arr);
    }

    public static void main(String[] args) {
        new Query7Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
