package SQL.queries;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ15 implements TestQ {

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query15().execute(tpchStreamDB);
        var imperative = new Query15Imperative().execute(tpchStreamDB);
        var imperativeLoopFusion = new Query15ImperativeLoopFusion().execute(tpchStreamDB);
        var imperativeLoopFusionLarge = new Query15ImperativeLargeLoopFusion().execute(tpchStreamDB);


        var joinMapMulti = new Query15JoinMapMulti().execute(tpchStreamDB);
        var partialResult = new Query15SqlGroupBy().execute(tpchStreamDB);
        var partialResultJoinMapMulti = new Query15SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);


        // check size
        int size = stream.size();
        assertEquals(size, imperative.size());
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, imperativeLoopFusion.size());
        assertEquals(size, imperativeLoopFusionLarge.size());
        assertEquals(size, partialResult.size());
        assertEquals(size, partialResultJoinMapMulti.size());

        // check entries
        var s = stream.toString();
        assertEquals(s, imperative.toString());
        assertEquals(s, imperativeLoopFusion.toString());
        assertEquals(s, imperativeLoopFusionLarge.toString());
        assertEquals(s, partialResult.toString());
        assertEquals(s, partialResultJoinMapMulti.toString());
        assertEquals(s, joinMapMulti.toString());
    }
}
