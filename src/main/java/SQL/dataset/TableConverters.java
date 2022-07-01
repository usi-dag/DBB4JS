package SQL.dataset;

import SQL.datatypes.Date;

import java.util.function.Function;

public class TableConverters {

    static final Function<String[], LineItemRow> lineitem = strings -> {
        try {
            var orderKey = Long.parseLong(strings[0]);
            var partKey = Long.parseLong(strings[1]);
            var suppKey = Long.parseLong(strings[2]);
            var lineNumber = Long.parseLong(strings[3]);
            var quantity = Double.parseDouble(strings[4]);
            var extendedPrice = Double.parseDouble(strings[5]);
            var discount = Double.parseDouble(strings[6]);
            var tax = Double.parseDouble(strings[7]);
            var shipDate = new Date(strings[10]);
            var commitDate = new Date(strings[11]);
            var receiptDate = new Date(strings[12]);
            return new LineItemRow.Record(orderKey, partKey, suppKey, lineNumber, quantity, extendedPrice, discount, tax,
                    strings[8], strings[9], shipDate, commitDate, receiptDate, strings[13], strings[14], strings[15]);
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    };

    static final Function<String[], CustomerRow> customer = strings -> {
        try {
            var custKey = Long.parseLong(strings[0]);
            var nationKey = Integer.parseInt(strings[3]);
            var acctBal = Double.parseDouble(strings[5]);
            return new CustomerRow.Record(custKey, strings[1], strings[2], nationKey, strings[4], acctBal, strings[6], strings[7]);
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    };

    static final Function<String[], NationRow> nation = strings -> {
        try {
            int nationKey = Integer.parseInt(strings[0]);
            int regionKey = Integer.parseInt(strings[2]);
            return new NationRow.Record(nationKey, strings[1], regionKey, strings[3]);
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    };

    static final Function<String[], OrdersRow> orders = strings -> {
        try {
            var orderKey = Long.parseLong(strings[0]);
            var custKey = Long.parseLong(strings[1]);
            var totalPrice = Double.parseDouble(strings[3]);
            var orderDate = new Date(strings[4]);
            var shipPriority = Integer.parseInt(strings[7]);
            return new OrdersRow.Record(orderKey, custKey, strings[2], totalPrice, orderDate, strings[5], strings[6], shipPriority, strings[8]);
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    };

    static final Function<String[], PartRow> part = strings -> {
        try {
            var partKey = Long.parseLong(strings[0]);
            var size = Integer.parseInt(strings[5]);
            var retailPrice = Double.parseDouble(strings[7]);
            return new PartRow.Record(partKey, strings[1], strings[2], strings[3], strings[4], size, strings[6], retailPrice, strings[8]);
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    };

    static final Function<String[], PartSuppRow> partsupp = strings -> {
        try {
            var a = Long.parseLong(strings[0]);
            var b = Long.parseLong(strings[1]);
            var c = Long.parseLong(strings[2]);
            var d = Double.parseDouble(strings[3]);
            return new PartSuppRow.Record(a, b, c, d, strings[4]);
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    };

    static final Function<String[], RegionRow> region = strings -> {
        try {
            int regionKey = Integer.parseInt(strings[0]);
            return new RegionRow.Record(regionKey, strings[1], strings[2]);
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    };

    static final Function<String[], SupplierRow> supplier = strings -> {
        try {
            var suppKey = Long.parseLong(strings[0]);
            var nationKey = Integer.parseInt(strings[3]);
            var accbal = Double.parseDouble(strings[5]);
            return new SupplierRow.Record(suppKey, strings[1], strings[2], nationKey, strings[4], accbal, strings[6]);
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
            return null;
        }
    };
}
