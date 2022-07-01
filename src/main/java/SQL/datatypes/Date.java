package SQL.datatypes;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class Date implements Comparable<Date> {
    final int days;

    public Date(String s) {
        int x;
        try {
            x = (int) LocalDate.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochDay();
        } catch (DateTimeParseException e) {
            e.printStackTrace();
            x = (int) LocalDate.now().toEpochDay();
        }
        this.days = x;
    }

    /*
    Only used for testing
     */
    public Date(int days) {
        this.days = days;
    }


    /**
     * If the difference between the days of the two dates is positive, then the first date is greater than the second
     * date. If the difference is negative, then the first date is less than the second date. If the difference is zero,
     * then the two dates are equal
     *
     * @param o The date to be compared.
     * @return The difference between the days of the two dates.
     */
    @Override
    public int compareTo(Date o) {
        return this.days - o.days;
    }

    @Override
    public String toString() {
        return LocalDate.ofEpochDay(days).toString();
    }

    public int getYears() {
        return LocalDate.ofEpochDay(days).getYear();
    }

    /**
     * This function is equivalent to curr >= min and curr < max
     *
     * @param minDate
     * @param maxDate
     * @return true if curr >= min and curr < max
     */
    public boolean fromTo(Date minDate, Date maxDate) {
        return this.compareTo(minDate) >= 0 && this.compareTo(maxDate) < 0;
    }
}
