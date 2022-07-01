package SQL.dataset;

import static org.junit.jupiter.api.Assertions.*;

import SQL.datatypes.Date;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Objects;


public class FileReaderTest {
    final TPCHStreamDB t = TPCHStreamDB.get();

    @Nested
    public class CustomerTest {

        @Test
        public void readFile() {
            assertEquals(1500, t.customer().filter(Objects::nonNull).count());
        }

        @Test
        public void checkAnElement() {
            var optCurr = t.customer().findFirst();
            assertTrue(optCurr.isPresent());
            var curr = optCurr.get();
            assertEquals(1, curr.c_custkey());
            assertEquals("Customer#000000001", curr.c_name());
            assertEquals("IVhzIApeRb ot,c,E", curr.c_address());
            assertEquals(15, curr.c_nationkey());
            assertEquals("25-989-741-2988", curr.c_phone());
            assertEquals(711.56, curr.c_acctbal());
            assertEquals("BUILDING", curr.c_mktsegment());
            assertEquals("to the even, regular platelets. regular, ironic epitaphs nag e", curr.c_comment());
        }
    }


    @Nested
    public class LineItemTest {


        @Test
        public void readFile() {
            assertEquals(60175, t.lineitem().filter(Objects::nonNull).count());

        }

        @Test
        public void checkAnElement() {
            var curr = t.lineitem().findFirst().get();
            assertEquals(1, curr.l_orderkey());
            assertEquals(1552, curr.l_partkey());
            assertEquals(93, curr.l_suppkey());
            assertEquals(1, curr.l_linenumber());
            assertEquals(17, curr.l_quantity());
            assertEquals(24710.35, curr.l_extendedprice());
            assertEquals(0.04, curr.l_discount());
            assertEquals(0.02, curr.l_tax());
            assertEquals("N", curr.l_returnflag());
            assertEquals("O", curr.l_linestatus());
            assertEquals(0, curr.l_shipdate().compareTo(new Date(9568)));
            assertEquals(0, curr.l_commitdate().compareTo(new Date(9538)));
            assertEquals(0, curr.l_receiptdate().compareTo(new Date(9577)));
            assertEquals("DELIVER IN PERSON", curr.l_shipinstruct());
            assertEquals("TRUCK", curr.l_shipmode());
            assertEquals("egular courts above the", curr.l_comment());

        }
    }


    @Nested
    public class NationTest {


        @Test
        public void readFile() {
            assertEquals(25, t.nation().filter(Objects::nonNull).count());

        }
    }


    @Nested
    public class OrdersTest {


        @Test
        public void readFile() {
            assertEquals(15000, t.orders().filter(Objects::nonNull).count());

        }
    }

    @Nested
    public class PartTest {


        @Test
        public void readFile() {
            assertEquals(2000, t.part().filter(Objects::nonNull).count());

        }
    }


    @Nested
    public class PartSuppTest {


        @Test
        public void readFile() {
            assertEquals(8000, t.partsupp().filter(Objects::nonNull).count());
        }
    }


    @Nested
    public class RegionTest {


        @Test
        public void readFile() {
            assertEquals(5, t.region().filter(Objects::nonNull).count());

        }
    }


    @Nested
    public class SupplierTest {


        @Test
        public void readFile() {
            assertEquals(100, t.supplier().filter(Objects::nonNull).count());

        }
    }
}
