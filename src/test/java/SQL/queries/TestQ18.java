package SQL.queries;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;


import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ18 implements TestQ {


    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query18().execute(tpchStreamDB);
        var imperative = new Query18Imperative().execute(tpchStreamDB);
        var imperativeLoopFusion = new Query18ImperativeLoopFusion().execute(tpchStreamDB);
        var joinMapMulti = new Query18JoinMapMulti().execute(tpchStreamDB);
        var sqlGroupBy = new Query18SqlGroupBy().execute(tpchStreamDB);
        var sqlGroupByJoinMapMulti = new Query18SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);
        var parallel = new Query18Parallel().execute(tpchStreamDB);


        int size = stream.size();
        // check size
        assertEquals(size, imperative.size());
        assertEquals(size, imperativeLoopFusion.size() );
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, sqlGroupBy.size());
        assertEquals(size, sqlGroupByJoinMapMulti.size());
        assertEquals(size, parallel.size());

        // check entries
        var s  = stream.toString();


        assertEquals(s, sqlGroupBy.toString());
        assertEquals(s, joinMapMulti.toString());
        assertEquals(s, sqlGroupByJoinMapMulti.toString());

        try {
            assertEquals(stream.toString(), imperative.toString());
        } catch (AssertionFailedError ef) {
            for (int i = 0; i < size; i++) {
                var a = (Query18.Accumulator) stream.get(i);
                var c = (Query18Imperative.Result) imperative.get(i);
                var p = (Query18Imperative.Result) parallel.get(i);

                try {
                    assertEquals(a.toString(), c.toString());
                } catch (AssertionFailedError e) {
                    if (!isARoundingError(a.quantity, c.quantity) || !a.c_name.equals(c.c_name) || a.o_orderkey != c.o_orderkey ||
                            a.o_orderdate.compareTo(c.o_orderdate) != 0 || a.o_totalprice != c.o_totalprice) {
                        throw e;
                    }
                }

                try {
                    assertEquals(a.toString(), p.toString());
                } catch (AssertionFailedError e){
                    if (!isARoundingError(a.quantity, p.quantity) || !a.c_name.equals(p.c_name) || a.o_orderkey != p.o_orderkey ||
                            a.o_orderdate.compareTo(p.o_orderdate) != 0 || a.o_totalprice != p.o_totalprice) {
                        throw e;
                    }
                }
            }
        }
        assertEquals(imperative.toString(), imperativeLoopFusion.toString());




    }

}
