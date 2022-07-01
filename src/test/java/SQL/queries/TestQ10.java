package SQL.queries;


import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQ10 implements TestQ {

    @Test
    @Override
    public void testCorrectness() {
        var stream = new Query10().execute(tpchStreamDB);
        var imperative = new Query10Imperative().execute(tpchStreamDB);
        var partialResult = new Query10SqlGroupBy().execute(tpchStreamDB);
        var joinMapMulti = new Query10JoinMapMulti().execute(tpchStreamDB);
        var partialResultJoinMultiMap = new Query10SqlGroupByAndJoinMapMulti().execute(tpchStreamDB);

        int size = imperative.size();
        assertEquals(size, imperative.size());
        assertEquals(size, partialResult.size());
        assertEquals(size, joinMapMulti.size());
        assertEquals(size, partialResultJoinMultiMap.size());


        var s = stream.toString();
        assertEquals(s, partialResult.toString());
        assertEquals(s, joinMapMulti.toString());
        assertEquals(s, partialResultJoinMultiMap.toString());

        // check entries
        for (int i = 0; i < size; i++) {
            var a = (Query10.Result) stream.get(i);
            var b = (Query10Imperative.Result) imperative.get(i);

            try {
                assertEquals(a.toString(),b.toString());
            } catch (AssertionFailedError e){
                if(!isARoundingError(a.revenue, b.revenue) && checkEntries(a,b)){
                    throw e;
                }
            }
        }
    }

    public boolean checkEntries(Query10.Result a , Query10Imperative.Result b){
        return a.c_custkey != b.c_custkey || !a.c_name.equals(b.c_name) || a.c_acctbal != b.c_acctbal ||
                !a.n_name.equals(b.n_name) || !a.c_address.equals(b.c_address) || !a.c_phone.equals(b.c_phone) ||
                !a.c_comment.equals(b.c_comment);
    }

}
