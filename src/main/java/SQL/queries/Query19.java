package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.PartRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;

public class Query19 implements Query {

    static class Result {
        double revenue;

        public void aggregate(LinePart l) {
            revenue += l.l_extendedprice * (1 - l.l_discount);
        }

        @Override
        public String toString() {
            return "Result{" +
                    "revenue=" + revenue +
                    '}';
        }
    }

    record LinePart(
            String p_brand,
            String p_container,
            double l_quantity,
            int p_size,
            String l_shipmode,
            String l_shipinstruct,
            double l_extendedprice,
            double l_discount
    ) {
    }

    // shipmode and l-shipinstruct
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Set<String> container_a = Set.of("SM CASE", "SM BOX", "SM PACK", "SM PKG");
        Set<String> container_b = Set.of("MED BAG", "MED BOX", "MED PKG", "MED PACK");
        Set<String> container_c = Set.of("LG CASE", "LG BOX", "LG PACK", "LG PKG");

        var LineItemPartJoin = SQLStreamUtils.streamJoin(
                        db.part(),
                        PartRow::p_partkey,
                        db.lineitem()
                                .filter(l -> (l.l_shipmode().equals("AIR") || l.l_shipmode().equals("AIR REG")))
                                .filter(l -> l.l_shipinstruct().equals("DELIVER IN PERSON")),
                        LineItemRow::l_partkey,
                        (p, l) -> new LinePart(p.p_brand(), p.p_container(), l.l_quantity(), p.p_size(), l.l_shipmode(), l.l_shipinstruct(), l.l_extendedprice(), l.l_discount())
                )
                .filter(l -> statement(l, "Brand#12", container_a, 1, 11, 1, 5) ||
                        statement(l, "Brand#23", container_b, 10, 20, 1, 10) ||
                        statement(l, "Brand#34", container_c, 20, 30, 1, 15)
                )
                .collect(Collector.of(
                        Result::new, Result::aggregate, (a, b) -> null
                ));
        return Collections.singletonList(LineItemPartJoin);
    }


    public static boolean statement(LinePart l, String brand, Set<String> container, int lowerQty, int upperQty,
                                    int lowerSize, int upperSize) {
        return l.p_brand().equals(brand) &&
                container.contains(l.p_container()) &&
                l.l_quantity() >= lowerQty && l.l_quantity() <= upperQty &&
                l.p_size() >= lowerSize && l.p_size() <= upperSize;
    }

    public static void main(String[] args) {
        new Query19().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
