package SQL.queries;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ11 implements TestQ {

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query11().execute(tpchStreamDB);
        var imperative = new Query11Imperative().execute(tpchStreamDB);
        var joinMapMulti = new Query11JoinMapMulti().execute(tpchStreamDB);
        var partialResult = new Query11SqlGroupBy().execute(tpchStreamDB);
        var partialResultJoinMapMulti = new Query11SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);

        // check size
        int size = stream.size();
        assertEquals(size, imperative.size());
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, partialResult.size());
        assertEquals(size, partialResultJoinMapMulti.size());

        var s = stream.toString();
        assertEquals(s, joinMapMulti.toString());
        assertEquals(s, partialResult.toString());
        assertEquals(s, partialResultJoinMapMulti.toString());
        // check entries
        try {
            assertEquals(s, imperative.toString());
        } catch (AssertionFailedError e){
            for (int i = 0; i < size; i++) {
                var a = (Query11.OuterSelect) stream.get(i);
                var b = (Query11Imperative.Result) imperative.get(i);
                assertEquals(a.value, b.value, 0.0001F);
                assertEquals(a.ps_partkey, b.ps_partkey);
            }
        }

    }
}
