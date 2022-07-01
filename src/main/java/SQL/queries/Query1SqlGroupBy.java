package SQL.queries;

import SQL.dataset.LineItemRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.sql_utils.GroupByCollector;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.tuple_types.StringStringPair;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;

public class Query1SqlGroupBy implements Query {


    public List<?> execute(TPCHStreamDB db) {

        var date = LocalDate.parse("1998-12-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        Date min = new Date(date.minusDays(90).toString());

        // comparator
        Comparator<Query1Imperative.Result> c1 = Comparator.comparing(r -> r.l_returnflag);
        Comparator<Query1Imperative.Result> c2 = Comparator.comparing(r -> r.l_linestatus);
        Comparator<Query1Imperative.Result> comparator = c1.thenComparing(c2);


        return db.lineitem()
                .filter(l -> l.l_shipdate().compareTo(min) <= 0)
                .collect(SQLCollectors.groupingBy(
                        x -> new StringStringPair(x.l_returnflag(), x.l_linestatus()),
                        GroupByCollector.of(
                                (LineItemRow l) -> new Query1Imperative.Result(l.l_returnflag(), l.l_linestatus()),
                                Query1Imperative.Result::aggregate,
                                Query1Imperative.Result::combine,
                                Query1Imperative.Result::finalizer
                        )))
                .values()
                .stream()
                .sorted(comparator)
                .toList();


    }

    public static void main(String[] args) {
        new Query1SqlGroupBy().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
