package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.OrdersRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.sql_utils.SQLStreamUtils;
import SQL.sql_utils.tuple_types.StringStringPair;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query12SingleFilterAndJoinMapMulti implements Query {

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Date min = new Date("1994-01-01");
        Date max = new Date("1995-01-01");
        Comparator<Query12.Result> comparator = Comparator.comparing(row -> row.l_shipmode);
        return SQLStreamUtils
                .streamJoinMapMulti(
                        db.lineitem()
                                .filter(l -> (l.l_shipmode().equals("MAIL") || l.l_shipmode().equals("SHIP"))
                                        && l.l_commitdate().compareTo(l.l_receiptdate()) < 0
                                        && l.l_shipdate().compareTo(l.l_commitdate()) < 0
                                        && l.l_receiptdate().fromTo(min, max)

                                )
                        ,
                        LineItemRow::l_orderkey,
                        db.orders(),
                        OrdersRow::o_orderkey,
                        (l, o) -> new StringStringPair(l.l_shipmode(), o.o_orderpriority())
                ).collect(Collectors.groupingBy(StringStringPair::fst,
                        Collector.of(
                                Query12.Result::new, Query12.Result::aggregate, (a, b) -> null)
                ))
                .values()
                .stream()
                .sorted(comparator)
                .toList();
    }

    public static void main(String[] args) {
        new Query12SingleFilterAndJoinMapMulti().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
