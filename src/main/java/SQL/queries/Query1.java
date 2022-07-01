package SQL.queries;

import SQL.dataset.Query;
import SQL.dataset.TPCHStreamDB;
import SQL.datatypes.Date;
import SQL.dataset.LineItemRow;
import SQL.sql_utils.tuple_types.StringStringPair;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Query1 implements Query {

    static class Result {
        public String l_returnflag;
        public String l_linestatus;
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        double count_order;


        public static void add(Result a, LineItemRow b) {
            a.l_linestatus = b.l_linestatus();
            a.l_returnflag = b.l_returnflag();
            a.sum_qty += b.l_quantity();
            a.sum_base_price += b.l_extendedprice();
            a.sum_disc_price += b.l_extendedprice() * (1.d - b.l_discount());
            a.sum_charge += b.l_extendedprice() * (1.d - b.l_discount()) * (1.d + b.l_tax());
            a.count_order += 1;
            a.avg_qty += b.l_quantity();
            a.avg_price += b.l_extendedprice();
            a.avg_disc += b.l_discount();
        }

        public Result combine(Result other) {
            sum_qty += other.sum_qty;
            sum_base_price += other.sum_base_price;
            sum_disc_price += other.sum_disc_price;
            sum_charge += other.sum_charge;
            count_order += other.count_order;
            avg_qty += other.avg_qty;
            avg_price += other.avg_price;
            avg_disc += other.avg_disc;
            return this;
        }

        public Result finalizer() {
            this.avg_qty = this.avg_qty / this.count_order;
            this.avg_price = this.avg_price / this.count_order;
            this.avg_disc = this.avg_disc / this.count_order;
            return this;
        }


        @Override
        public String toString() {
            return "Result{" +
                    "l_returnflag='" + l_returnflag + '\'' +
                    ", l_linestatus='" + l_linestatus + '\'' +
                    ", sum_qty=" + sum_qty +
                    ", sum_base_price=" + sum_base_price +
                    ", sum_disc_price=" + sum_disc_price +
                    ", sum_charge=" + sum_charge +
                    ", avg_qty=" + avg_qty +
                    ", avg_price=" + avg_price +
                    ", avg_disc=" + avg_disc +
                    ", count_order=" + count_order +
                    '}';
        }
    }


    public List<?> execute(TPCHStreamDB db) {

        var date = LocalDate.parse("1998-12-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        Date min = new Date(date.minusDays(90).toString());

        // comparator
        Comparator<Result> c1 = Comparator.comparing(r -> r.l_returnflag);
        Comparator<Result> c2 = Comparator.comparing(r -> r.l_linestatus);
        Comparator<Result> comparator = c1.thenComparing(c2);


        return db.lineitem()
                .sequential()
                .filter(x -> x.l_shipdate().compareTo(min) <= 0)
                .collect(Collectors.groupingBy(
                        x -> new StringStringPair(x.l_returnflag(), x.l_linestatus()),
                        Collector.of(Result::new,
                                Result::add,
                                Result::combine,
                                Result::finalizer)))

                // note: stream breaks here, run a new stream with the values in the map
                .values()
                .stream()
                .sorted(comparator)
                .toList();

    }


    public static void main(String[] args) {
        new Query1().execute(TPCHStreamDB.get()).forEach(System.out::println);
    }
}
