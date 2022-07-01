package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;

import java.util.*;

public class Query10Imperative implements Query {

    record NationCustOrder(long c_custkey, String c_name, double c_acctbal, String n_name, String c_address,
                           String c_phone, String c_comment) {
    }

    record Q10Key(long c_custkey, String c_name, double c_acctbal, String c_phone, String n_name, String c_address,
                  String c_comment) {
    }

    static class Result {
        final long c_custkey;
        final String c_name;
        double revenue;
        final double c_acctbal;
        final String n_name;
        final String c_address;
        final String c_phone;
        final String c_comment;

        public Result(long c_custkey, String c_name, double c_acctbal, String n_name, String c_address, String c_phone, String c_comment) {
            this.c_custkey = c_custkey;
            this.c_name = c_name;
            this.c_acctbal = c_acctbal;
            this.n_name = n_name;
            this.c_address = c_address;
            this.c_phone = c_phone;
            this.c_comment = c_comment;
        }

        public void aggregate(double l_extendendprice, double l_discount) {
            revenue += l_extendendprice * (1.D - l_discount);
        }

        @Override
        public String toString() {
            return "Result{" +
                    "c_custkey=" + c_custkey +
                    ", c_name='" + c_name + '\'' +
                    ", revenue=" + revenue +
                    ", c_acctbal=" + c_acctbal +
                    ", n_name='" + n_name + '\'' +
                    ", c_address='" + c_address + '\'' +
                    ", c_phone='" + c_phone + '\'' +
                    ", c_comment='" + c_comment + '\'' +
                    '}';
        }
    }


    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1993-10-01");
        Date max = new Date("1994-01-01");

        // left side
        Map<Integer, List<NationRow>> nationMap = new HashMap<>();
        for (NationRow n : db.nation_arr()) {
            nationMap.computeIfAbsent(n.n_nationkey(), k -> new ArrayList<>()).add(n);
        }

        Map<Long, List<OrdersRow>> orderKeyMap = new HashMap<>();
        for (OrdersRow o : db.orders_arr()) {
            if (o.o_orderdate().fromTo(min, max)) {
                var value = orderKeyMap.computeIfAbsent(o.o_custkey(), k -> new ArrayList<>());
                value.add(o);
            }
        }

        Map<Long, List<NationCustOrder>> nationCustOrderMap = new HashMap<>();
        for (CustomerRow c : db.customer_arr()) {
            var getList = orderKeyMap.get(c.c_custkey());
            var custkey = c.c_custkey();
            var cname = c.c_name();
            var acctbal = c.c_acctbal();
            var address = c.c_address();
            var phone = c.c_phone();
            var comment = c.c_comment();
            Integer nationkey = c.c_nationkey();
            if (getList != null) {
                for (OrdersRow o : getList) {
                    var list2 = nationMap.get(nationkey);
                    if (list2 != null) {
                        var get = nationCustOrderMap.computeIfAbsent(o.o_orderkey(), k -> new ArrayList<>());
                        for (var n : list2) {
                            get.add(new NationCustOrder(custkey, cname, acctbal, n.n_name(), address, phone, comment));
                        }
                    }
                }
            }
        }


        Map<Q10Key, Result> finalMap = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            if (l.l_returnflag().equals("R")) {
                var getList = nationCustOrderMap.get(l.l_orderkey());
                if (getList != null) {
                    var toAdd = l.l_extendedprice() * (1 - l.l_discount());
                    for (NationCustOrder nco : getList) {
                        var key = new Q10Key(nco.c_custkey(), nco.c_name(), nco.c_acctbal(), nco.c_phone(), nco.n_name(), nco.c_address(), nco.c_comment());
                        var result = finalMap.computeIfAbsent(key, k -> new Result(
                                nco.c_custkey(), nco.c_name(), nco.c_acctbal(), nco.n_name(), nco.c_address(), nco.c_phone(), nco.c_comment()
                        ));
                        result.revenue += toAdd;
                    }
                }
            }
        }

        Comparator<Result> comparator = Comparator.comparing(result -> -result.revenue);
        var arr = finalMap.values().toArray(Result[]::new);
        Arrays.sort(arr, comparator);
        if (arr.length > 20) {
            return Arrays.asList(arr).subList(0, 20);
        } else {
            return Arrays.asList(arr);
        }

    }

    public static void main(String[] args) {
        new Query10Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
