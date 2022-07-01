package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.PartRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;

import java.util.*;
import java.util.stream.Collector;

public class Query14 implements Query {
    record Q14JoinedRow(String p_type, double l_extendedprice, double l_discount) {
    }

    static class Result {
        double promoRevenue;
        double totalRevenue;

        void accumulate(Q14JoinedRow row) {
            double x = row.l_extendedprice() * (1 - row.l_discount());
            totalRevenue += x;
            if (row.p_type().startsWith("PROMO")) {
                promoRevenue += x;
            }
        }

        @Override
        public String toString() {
            return "Result{" +
                    "promo_revenue=" + promoRevenue +
                    '}';
        }

        public Result finalizer() {
            promoRevenue = 100 * promoRevenue / totalRevenue;
            return this;
        }
    }

    public List<?> execute(TPCHStreamDB db) {
        var dateFrom = new Date("1995-09-01");
        var dateTo = new Date("1995-10-01");

        var lineItemPartJoin = SQLStreamUtils.streamJoin(
                        db.lineitem().filter(x -> x.l_shipdate().compareTo(dateFrom) >= 0)
                                .filter(x -> x.l_shipdate().compareTo(dateTo) < 0),
                        LineItemRow::l_partkey,
                        db.part(),
                        PartRow::p_partkey,
                        (l, p) -> new Q14JoinedRow(
                                p.p_type(), l.l_extendedprice(), l.l_discount())
                )
                .collect(Collector.of(
                        Result::new,
                        Result::accumulate,
                        (x, y) -> null, // no combiner needed
                        Result::finalizer));

        return Collections.singletonList(lineItemPartJoin);


    }

    public static void main(String[] args) {
        new Query14().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
