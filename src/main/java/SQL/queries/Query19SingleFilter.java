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

import static SQL.queries.Query19.statement;

public class Query19SingleFilter implements Query {
    @Override
    public List<?> execute(TPCHStreamDB db) {
        Set<String> container_a = Set.of("SM CASE", "SM BOX", "SM PACK", "SM PKG");
        Set<String> container_b = Set.of("MED BAG", "MED BOX", "MED PKG", "MED PACK");
        Set<String> container_c = Set.of("LG CASE", "LG BOX", "LG PACK", "LG PKG");

        var LineItemPartJoin = SQLStreamUtils.streamJoin(
                        db.part(),
                        PartRow::p_partkey,
                        db.lineitem().filter(l -> (l.l_shipmode().equals("AIR") || l.l_shipmode().equals("AIR REG")) && l.l_shipinstruct().equals("DELIVER IN PERSON")),
                        LineItemRow::l_partkey,
                        (p, l) -> new Query19.LinePart(p.p_brand(), p.p_container(), l.l_quantity(), p.p_size(), l.l_shipmode(), l.l_shipinstruct(), l.l_extendedprice(), l.l_discount())
                )
                .filter(l -> statement(l, "Brand#12", container_a, 1, 11, 1, 5) ||
                        statement(l, "Brand#23", container_b, 10, 20, 1, 10) ||
                        statement(l, "Brand#34", container_c, 20, 30, 1, 15)
                )
                .collect(Collector.of(
                        Query19.Result::new, Query19.Result::aggregate, (a, b) -> null
                ));
        return Collections.singletonList(LineItemPartJoin);
    }
}
