package SQL.queries;

import SQL.dataset.*;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query8 implements Query {

    record LineItemOrder(Date o_orderdate, long o_custkey, double l_extendedprice, double l_discount, long l_suppkey) {
    }

    record LineItemOrderCustomer(Date o_orderdate, int c_nationkey, double l_extendedprice, double l_discount,
                                 long l_suppkey) {
    }

    record LineItemOrderCustomerSupp(Date o_orderdate, int s_nationkey, double l_extendedprice, double l_discount) {
    }

    record Last(Date o_orderdate, double l_extendedprice, double l_discount, String nation) {
    }


    static class Result {
        int o_years;
        double mkt_share;
        double b;


        public void aggregate(Last l) {
            o_years = l.o_orderdate.getYears();
            var toAdd = (l.l_extendedprice() * (1 - l.l_discount()));
            mkt_share += l.nation().equals("BRAZIL") ? toAdd : 0;
            b += toAdd;
        }

        public Result finalizer() {
            this.mkt_share = this.mkt_share / this.b;
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

    /**
     * We join the `part` table with the `lineitem` table, then we join the result with the `orders` table, then we join
     * the result with the `customer` table, then we join the result with the `region` table, then we join the result with
     * the `nation` table, then we join the result with the `supplier` table, then we join the result with the `nation`
     * table again, and finally we group by the year and aggregate the results
     *
     * @param db TPCHStreamDB is a class that contains all the tables in the TPCH database.
     * @return A list of Result objects.
     */
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1995-01-01");
        Date max = new Date("1996-12-31");
        // part and lineItem join
        var partLineItemJoin = SQLStreamUtils.streamJoin(
                db.part().filter(p -> p.p_type().equals("ECONOMY ANODIZED STEEL")),
                PartRow::p_partkey,
                db.lineitem(),
                LineItemRow::l_partkey,
                (p, l) -> l
        );

        // lineItem orders join
        var lineItemOrdersJoin = SQLStreamUtils.streamJoin(
                partLineItemJoin,
                LineItemRow::l_orderkey,
                db.orders().filter(o -> o.o_orderdate().fromTo(min, max)),
                OrdersRow::o_orderkey,
                (l, o) -> new LineItemOrder(o.o_orderdate(), o.o_custkey(), l.l_extendedprice(), l.l_discount(), l.l_suppkey())
        );


        // LineItemOrder and Customer join
        var lineItemOrderCustomerJoin = SQLStreamUtils.streamJoin(
                lineItemOrdersJoin,
                LineItemOrder::o_custkey,
                db.customer(),
                CustomerRow::c_custkey,
                (lo, c) -> new LineItemOrderCustomer(lo.o_orderdate(), c.c_nationkey(), lo.l_extendedprice(), lo.l_discount(), lo.l_suppkey())
        );


        // region netion join (left part query plan)
        var regionNationJoin = SQLStreamUtils.streamJoin(
                db.region().filter(r -> r.r_name().equals("AMERICA")),
                RegionRow::r_regionkey,
                db.nation(),
                NationRow::n_regionkey,
                (r, n) -> n
        );


        // regionNation and lineItemOrderCustomer join
        var regionNationAndLineItemOrderCustomerJoin = SQLStreamUtils.streamJoin(
                regionNationJoin,
                NationRow::n_nationkey,
                lineItemOrderCustomerJoin,
                LineItemOrderCustomer::c_nationkey,
                (n, loc) -> loc
        );


        // join regionNationAndLineItemOrderCustomerJoin and supplier
        var regionNationAndLineItemOrderCustomerSupplierJoin = SQLStreamUtils.streamJoin(
                regionNationAndLineItemOrderCustomerJoin,
                LineItemOrderCustomer::l_suppkey,
                db.supplier(),
                SupplierRow::s_suppkey,
                (loc, s) -> new LineItemOrderCustomerSupp(loc.o_orderdate(), s.s_nationkey(), loc.l_extendedprice(), loc.l_discount())
        );

        Comparator<Result> comparator = Comparator.comparing(r -> r.o_years);

        // last join
        return SQLStreamUtils.streamJoin(
                        db.nation(),
                        NationRow::n_nationkey,
                        regionNationAndLineItemOrderCustomerSupplierJoin,
                        LineItemOrderCustomerSupp::s_nationkey,
                        (n, loc) -> new Last(loc.o_orderdate(), loc.l_extendedprice(), loc.l_discount(), n.n_name())
                ).collect(
                        Collectors.groupingBy(o -> o.o_orderdate().getYears(), Collector.of(
                                Result::new, Result::aggregate, (a, b) -> null, Result::finalizer))
                ).values()
                .stream()
                .sorted(comparator)
                .toList();

    }

    public static void main(String[] args) {
        new Query8().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
