package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;

import java.util.*;

public class Query8Imperative implements Query {

    record LineItemOrder(Date o_orderdate, double l_extendedprice, double l_discount, long l_suppkey) {
    }

    record LineItemOrderCustomer(Date o_orderdate, double l_extendedprice, double l_discount, int c_nationkey,
                                 long l_suppkey) {
    }


    static class Result {
        final int o_years;
        double mkt_share;
        double b;

        public Result(int o_years) {
            this.o_years = o_years;
        }


        public void aggregate(double l_extendeprice, double l_discount, String name) {
            var toAdd = l_extendeprice * (1 - l_discount);
            if (name.equals("BRAZIL")) {
                mkt_share += toAdd;
            }
            b += toAdd;
        }

        public Result finalizer() {
            mkt_share = mkt_share / b;
            return this;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "o_years=" + o_years +
                    ", mkt_share=" + mkt_share +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1995-01-01");
        Date max = new Date("1996-12-31");

        var nationArr = db.nation_arr();

        // right side
        Map<Long, List<PartRow>> partMap = new HashMap<>();
        for (PartRow p : db.part_arr()) {
            if (p.p_type().equals("ECONOMY ANODIZED STEEL")) {
                partMap.computeIfAbsent(p.p_partkey(), k -> new ArrayList<>()).add(p);
            }
        }

        Map<Long, List<LineItemRow>> lineItemMap = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            var list = partMap.get(l.l_partkey());
            if (list != null) {
                var get = lineItemMap.computeIfAbsent(l.l_orderkey(), k -> new ArrayList<>());
                get.add(l);
            }
        }
        Map<Long, List<LineItemOrder>> lineItemOrderMap = new HashMap<>();
        for (OrdersRow o : db.orders_arr()) {
            if (o.o_orderdate().fromTo(min, max)) {
                var list = lineItemMap.get(o.o_orderkey());
                if (list != null) {
                    var value = lineItemOrderMap.computeIfAbsent(o.o_custkey(), k -> new ArrayList<>());
                    var date = o.o_orderdate();
                    for (LineItemRow l : list) {
                        value.add(new LineItemOrder(date, l.l_extendedprice(), l.l_discount(), l.l_suppkey()));
                    }
                }
            }
        }


        // left side of query plan
        Map<Integer, List<RegionRow>> regionMap = new HashMap<>();
        for (RegionRow r : db.region_arr()) {
            if (r.r_name().equals("AMERICA")) {
                regionMap.computeIfAbsent(r.r_regionkey(), k -> new ArrayList<>()).add(r);
            }
        }

        Map<Integer, List<NationRow>> regionNationMap = new HashMap<>();
        for (NationRow n : nationArr) {
            var list = regionMap.get(n.n_regionkey());
            if (list != null) {
                regionNationMap.computeIfAbsent(n.n_nationkey(), k -> new ArrayList<>()).add(n);
            }
        }


        // incontro del left side and right side
        Map<Long, List<LineItemOrderCustomer>> lineItemOrderCustomerMap = new HashMap<>();
        for (CustomerRow c : db.customer_arr()) {
            var list = lineItemOrderMap.get(c.c_custkey());
            if (list != null) {
                var cn = c.c_nationkey();
                for (var l : list) {
                    var list2 = regionNationMap.get(cn);
                    if (list2 != null) {
                        lineItemOrderCustomerMap.computeIfAbsent(l.l_suppkey(), k -> new ArrayList<>())
                                .add(new LineItemOrderCustomer(l.o_orderdate(), l.l_extendedprice(), l.l_discount(), cn, l.l_suppkey()));
                    }
                }
            }
        }

        Map<Integer, List<NationRow>> lastNationMap = new HashMap<>();
        for (NationRow n : nationArr) {
            lastNationMap.computeIfAbsent(n.n_nationkey(), k -> new ArrayList<>()).add(n);
        }
        Map<Integer, Result> resultMap = new HashMap<>();

        for (SupplierRow s : db.supplier_arr()) {
            var list = lineItemOrderCustomerMap.get(s.s_suppkey());
            if (list != null) {
                var sn = s.s_nationkey();
                for (var l : list) {
                    var list2 = lastNationMap.get(sn);
                    if (list2 != null) {
                        Integer years = l.o_orderdate().getYears();
                        var toAdd = l.l_extendedprice() * (1 - l.l_discount());
                        for (var n : list2) {
                            var result = resultMap.computeIfAbsent(years, k -> new Result(years));
                            if (n.n_name().equals("BRAZIL")) {
                                result.mkt_share += toAdd;
                            }
                            result.b += toAdd;

                        }

                    }
                }
            }
        }
        resultMap.values().forEach(r -> r.mkt_share = r.mkt_share / r.b);

        var arr = resultMap.values().toArray(Result[]::new);
        Comparator<Result> comparator = Comparator.comparing(r -> r.o_years);
        Arrays.sort(arr, comparator);
        return Arrays.asList(arr);

    }

    public static void main(String[] args) {
        new Query8Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
