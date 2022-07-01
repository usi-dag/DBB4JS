package SQL.queries;

import SQL.dataset.CustomerRow;
import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.sql_utils.MarkedArrayList;
import SQL.sql_utils.SQLCollectors;
import SQL.sql_utils.StringMultiContains;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Query13 implements Query {

    record OuterJoin(long c_custkey, Long o_ordekey) {
    }

    record Result(long c_count, long custdist) {
        @Override
        public String toString() {
            return "Result{" +
                    "c_count=" + c_count +
                    ", custdist=" + custdist +
                    '}';
        }
    }

    @Override
    public List<?> execute(TPCHStreamDB db) {
        Comparator<Result> c1 = Comparator.comparing(r -> -r.custdist);
        Comparator<Result> c2 = Comparator.comparing(r -> -r.c_count);
        Comparator<Result> comparator = c1.thenComparing(c2);

        var customerMap = db.customer()
                .collect(Collectors.groupingBy(CustomerRow::c_custkey, SQLCollectors.toMarkedList()));

        MarkedArrayList<OuterJoin> empty = new MarkedArrayList<>();

        StringMultiContains matcher = new StringMultiContains("special", "requests");
        var joinedCustomer = db.orders()
                .filter(o -> !matcher.match(o.o_comment()))
                .flatMap(o -> {
                    MarkedArrayList<CustomerRow> customers = customerMap.get(o.o_custkey());
                    if (customers == null) {
                        return empty.stream();
                    }
                    customers.mark();
                    return customers
                            .stream()
                            .map(c -> new OuterJoin(c.c_custkey(), o.o_orderkey()));
                });

        var nullJoinCustomer = customerMap
                .values()
                .stream()
                .filter(x -> !x.isMarked())
                .flatMap(Collection::stream)
                .map(c -> new OuterJoin(c.c_custkey(), null));

        return Stream.concat(joinedCustomer, nullJoinCustomer)
                .collect(Collectors.groupingBy(OuterJoin::c_custkey, Collectors.summingInt(
                        t -> t.o_ordekey != null ? 1 : 0)))
                .entrySet()
                .stream()
                .collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.counting()))
                .entrySet()
                .stream()
                .map(x -> new Result(x.getKey(), x.getValue()))
                .sorted(comparator)
                .toList();
    }


    public static void main(String[] args) {
        new Query13().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
