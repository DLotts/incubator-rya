package org.openrdf.query.algebra.evaluation.impl;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;

import com.google.common.collect.HashMultimap;

public class QueryUnCartesianProductOptimizerTest {

    @Test
    public void testGetVarBins01() {
        // Var varA=new Var("A");
        TupleExpr tupleExpr = new StatementPattern(new Var("A"), new Var("B"), new Var("C"));
        HashMultimap<String, StatementPattern> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);
        assertEquals("contains 3, A B and C vars.", 3, mm.size());
    }

    @Test
    public void testGetVarBins02() {
        Join tupleExpr = new Join(new Join(new Join(new Join(sp("ABC"), sp("BCD")), sp("DEF")), sp("FGH")), sp("HIJ"));
        HashMultimap<String, StatementPattern> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);
        assertEquals("contains 15, 5 sp *3 vars.=" + mm, 15, mm.size());
        assertEquals("A bin has 1.=" + mm.get("A"), 1, mm.get("A").size());
        assertEquals("B bin has 2.=" + mm.get("B"), 2, mm.get("B").size());
        assertEquals("J bin has 1.=" + mm.get("J"), 1, mm.get("J").size());
        assertEquals("X bin has 0.=" + mm.get("X"), 0, mm.get("X").size());
        assertEquals("ABC bin has 0.=" + mm.get("ABC"), 0, mm.get("ABC").size());
    }

    @Test
    public void testGetVarBins03() {
        Join tupleExpr = new Join(new Join(new Join(new Join(sp("ABC"), sp("BBB")), sp("DBE")), sp("EFB")), sp("BGB"));
        HashMultimap<String, StatementPattern> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);
        assertEquals("contains 12, 3 vars * 5 sp - 3 repeats: BBB,BGB .=" + mm, 12, mm.size());
        assertEquals("A bin has =" + mm.get("A"), 1, mm.get("A").size());
        assertEquals("B bin has =" + mm.get("B"), 5, mm.get("B").size());
        assertEquals("X bin has =" + mm.get("X"), 0, mm.get("X").size());
        assertEquals("ABC bin has 0.=" + mm.get("ABC"), 0, mm.get("ABC").size());
    }

    /**
     * Make a statement pattern easy.
     * 
     * @param s
     *            var
     * @param p
     *            var
     * @param o
     *            var
     * @return new sp
     */
    StatementPattern sp(String s, String p, String o) {
        return new StatementPattern(new Var(s), new Var(p), new Var(o));
    }

    /**
     * Make a statement pattern easy, provide var names as single letters
     * concatinated.
     * 
     * @param spo
     *            -- split first three chars as the binding variables.
     * @return a new statementpattern
     */
    StatementPattern sp(String spo) {
        return new StatementPattern(new Var(spo.substring(0, 1)), new Var(spo.substring(1, 2)),
                        new Var(spo.substring(2)));
    }

    @Test
    public void testMakeGraphLine() {
        Join tupleExpr = new Join(new Join(new Join(new Join(sp("ABC"), sp("BCD")), sp("DEF")), sp("FGH")), sp("HIJ"));
        HashMultimap<String, StatementPattern> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);
        
        HashMultimap<StatementPattern, StatementPattern> graph = QueryUnCartesianProductOptimizer.makeGraph(mm);
        assertEquals("contains nodes=" + graph, 5, graph.keySet().size());
        assertEquals("contains edges=" + graph, 8, graph.size());
    }

    @Test
    public void testMakeGraphCircle() {
        Join tupleExpr = new Join(new Join(new Join(new Join(sp("ABC"), sp("BCD")), sp("DEF")), sp("FGH")), sp("HIA"));
        HashMultimap<String, StatementPattern> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);

        HashMultimap<StatementPattern, StatementPattern> graph = QueryUnCartesianProductOptimizer.makeGraph(mm);
        assertEquals("contains nodes=" + graph, 5, graph.keySet().size());
        assertEquals("contains edges=" + graph, 10, graph.size());
    }

    @Test
    public void testMakeGraphFullyConnected() {
        Join tupleExpr = new Join(new Join(new Join(new Join(sp("ABC"), sp("BBB")), sp("DBE")), sp("EFB")), sp("BGB"));
        HashMultimap<String, StatementPattern> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);

        HashMultimap<StatementPattern, StatementPattern> graph = QueryUnCartesianProductOptimizer.makeGraph(mm);
        assertEquals("contains nodes=" + graph, 5, graph.keySet().size());
        assertEquals("contains edges=" + graph, 20, graph.size());
    }

}
