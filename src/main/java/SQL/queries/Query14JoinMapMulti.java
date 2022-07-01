package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.PartRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collector;

public class Query14JoinMapMulti implements Query {

    public List<?> execute(TPCHStreamDB db) {
        var dateFrom = new Date("1995-09-01");
        var dateTo = new Date("1995-10-01");

        var lineItemPartJoin = SQLStreamUtils.streamJoinMapMulti(
                        db.lineitem().filter(x -> x.l_shipdate().compareTo(dateFrom) >= 0)
                                .filter(x -> x.l_shipdate().compareTo(dateTo) < 0),
                        LineItemRow::l_partkey,
                        db.part(),
                        PartRow::p_partkey,
                        (l, p) -> new Query14.Q14JoinedRow(
                                p.p_type(), l.l_extendedprice(), l.l_discount())
                )
                .collect(Collector.of(
                        Query14.Result::new,
                        Query14.Result::accumulate,
                        (x, y) -> null, // no combiner needed
                        Query14.Result::finalizer));

        return Collections.singletonList(lineItemPartJoin);

    }

    public static void main(String[] args) {
        new Query14JoinMapMulti().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
