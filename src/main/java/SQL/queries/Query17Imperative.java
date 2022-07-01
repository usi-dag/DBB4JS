package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.PartRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;

import java.util.*;

public class Query17Imperative implements Query {

    static class Avg {

        final long l_partkey;
        int count;
        double avg;

        public Avg(long l_partkey) {
            this.l_partkey = l_partkey;
        }

        public void aggregate(double l_quantity) {
            avg += l_quantity;
            count++;
        }

        public Avg finalizer() {
            avg = (avg / count) * 0.2d;
            return this;
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Map<Long, List<PartRow>> partRowMap = new HashMap<>();
        for (PartRow p : db.part_arr()) {
            if (p.p_brand().equals("Brand#23") && p.p_container().equals("MED BOX")) {
                var list = partRowMap.computeIfAbsent(p.p_partkey(), k -> new ArrayList<>());
                list.add(p);
            }
        }


        Map<Long, Avg> lineItemMap = new HashMap<>();
        for (LineItemRow l : db.lineitem_arr()) {
            var list = lineItemMap.computeIfAbsent(l.l_partkey(), k -> new Avg(l.l_partkey()));
            list.count += 1;
            list.avg += l.l_quantity();
        }


        // part avg join (l_partkey)
        Map<Long, Avg> avgMap = new HashMap<>();
        for (var avg : lineItemMap.values()) {
            Long avgPartKey = avg.l_partkey;
            var list = partRowMap.get(avgPartKey);
            if (list != null) {
                avgMap.putIfAbsent(avgPartKey, avg.finalizer());
            }
        }


        // last join (lineItem join lineItem)
        double sum = 0;
        for (LineItemRow l : db.lineitem_arr()) {
            var get = avgMap.get(l.l_partkey());
            if (get != null && l.l_quantity() < get.avg) {
                sum += l.l_extendedprice();
            }
        }

        return Collections.singletonList(new Query17.Result(sum / 7.0D));
    }

    public static void main(String[] args) {
        new Query17Imperative().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
