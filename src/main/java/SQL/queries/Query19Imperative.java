package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.PartRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;

import java.util.*;

public class Query19Imperative implements Query {

    record Result(double revenue) {
        @Override
        public String toString() {
            return "Result{" +
                    "revenue=" + revenue +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Set<String> container_a = Set.of("SM CASE", "SM BOX", "SM PACK", "SM PKG");
        Set<String> container_b = Set.of("MED BAG", "MED BOX", "MED PKG", "MED PACK");
        Set<String> container_c = Set.of("LG CASE", "LG BOX", "LG PACK", "LG PKG");


        HashMap<Long, List<PartRow>> partMap = new HashMap<>();
        for (PartRow p : db.part_arr()) {
            var value = partMap.computeIfAbsent(p.p_partkey(), k -> new ArrayList<>());
            value.add(p);
        }
        double revenue = 0;
        for (LineItemRow l : db.lineitem_arr()) {
            if ((l.l_shipmode().equals("AIR") || l.l_shipmode().equals("AIR REG")) && l.l_shipinstruct().equals("DELIVER IN PERSON")) {

                var list = partMap.get(l.l_partkey());
                if (list != null) {
                    for (var p : list) {
                        if (statement(l, p, "Brand#12", container_a, 1, 11, 1, 5) ||
                                statement(l, p, "Brand#23", container_b, 10, 20, 1, 10) ||
                                statement(l, p, "Brand#34", container_c, 20, 30, 1, 15)) {
                            revenue += l.l_extendedprice() * (1 - l.l_discount());
                        }
                    }
                }
            }
        }

        return Collections.singletonList(new Result(revenue));
    }

    public static boolean statement(LineItemRow lineItem, PartRow part, String brand, Set<String> container, int lowerQty, int upperQty,
                                    int lowerSize, int upperSize) {
        return part.p_brand().equals(brand) &&
                container.contains(part.p_container()) &&
                lineItem.l_quantity() >= lowerQty && lineItem.l_quantity() <= upperQty &&
                part.p_size() >= lowerSize && part.p_size() <= upperSize;
    }

    public static void main(String[] args) {
        new Query19Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
