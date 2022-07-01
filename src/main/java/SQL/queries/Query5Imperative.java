package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.tuple_types.LongIntPair;
import SQL.sql_utils.tuple_types.StringIntPair;

import java.util.*;

public class Query5Imperative implements Query {


    static class Result {
        final String n_name;
        double revenue;

        public Result(String n_name) {
            this.n_name = n_name;
        }

        public void aggregate(double l_extendedprice, double l_discount) {
            revenue += l_extendedprice * (1 - l_discount);
        }

        @Override
        public String toString() {
            return "Result{" +
                    "n_name='" + n_name + '\'' +
                    ", revenue=" + revenue +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1994-01-01");
        Date max = new Date("1995-01-01");

        Map<Integer, List<RegionRow>> regionMap = new HashMap<>();
        for (RegionRow r : db.region_arr()) {
            if (r.r_name().equals("ASIA")) {
                var list = regionMap.computeIfAbsent(r.r_regionkey(), k -> new ArrayList<>());
                list.add(r);
            }
        }

        Map<Integer, List<NationRow>> nationRegionMap = new HashMap<>();
        for (NationRow n : db.nation_arr()) {
            var list = regionMap.get(n.n_regionkey());
            if (list != null) {
                nationRegionMap.computeIfAbsent(n.n_nationkey(), k -> new ArrayList<>()).add(n);
            }
        }

        Map<Long, List<StringIntPair>> nationCustomerMap = new HashMap<>();
        for (CustomerRow c : db.customer_arr()) {
            var list = nationRegionMap.get(c.c_nationkey());
            if (list != null) {
                var ncmList = nationCustomerMap.computeIfAbsent(c.c_custkey(), k -> new ArrayList<>());
                for (NationRow n : list) {
                    ncmList.add(new StringIntPair(n.n_name(), c.c_nationkey()));
                }
            }
        }

        Map<Long, List<StringIntPair>> nationCustOrderMap = new HashMap<>();
        for (OrdersRow o : db.orders_arr()) {
            if (o.o_orderdate().fromTo(min, max)) {
                var list = nationCustomerMap.get(o.o_custkey());
                if (list != null) {
                    var ncoList = nationCustOrderMap.computeIfAbsent(o.o_orderkey(), k -> new ArrayList<>());
                    ncoList.addAll(list);
                }
            }
        }

        Map<LongIntPair, List<SupplierRow>> supplierMap = new HashMap<>();
        for (SupplierRow s : db.supplier_arr()) {
            var list = supplierMap.computeIfAbsent(new LongIntPair(s.s_suppkey(), s.s_nationkey()), k -> new ArrayList<>());
            list.add(s);
        }
        Map<String, Result> finalMap = new HashMap<>();

        for (LineItemRow l : db.lineitem_arr()) {
            var list = nationCustOrderMap.get(l.l_orderkey());
            if (list != null) {
                for (StringIntPair nc : list) {
                    var list2 = supplierMap.get(new LongIntPair(l.l_suppkey(), nc.snd()));
                    if (list2 != null) {
                        var result = finalMap.computeIfAbsent(nc.fst(), k -> new Result(nc.fst()));
                        result.revenue += (l.l_extendedprice() * (1 - l.l_discount()));
                    }
                }
            }
        }

        Comparator<Result> comparator = Comparator.comparing(r -> -r.revenue);

        var arr = finalMap.values().toArray(Result[]::new);
        Arrays.sort(arr, comparator);
        return Arrays.asList(arr);


    }

    public static void main(String[] args) {
        new Query5Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
