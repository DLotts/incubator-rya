package org.openrdf.query.algebra.evaluation.impl;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

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
        HashMultimap<String, TupleExpr> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);
        assertEquals("contains 3, A B and C vars.", 3, mm.size());
    }

    @Test
    public void testGetVarBins02() {
        Join tupleExpr = new Join(new Join(new Join(new Join(sp("ABC"), sp("BCD")), sp("DEF")), sp("FGH")), sp("HIJ"));
        HashMultimap<String, TupleExpr> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);
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
        HashMultimap<String, TupleExpr> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);
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
        HashMultimap<String, TupleExpr> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);

        HashMultimap<TupleExpr, TupleExpr> graph = QueryUnCartesianProductOptimizer.makeGraph(mm);
        assertEquals("contains nodes=" + graph, 5, graph.keySet().size());
        assertEquals("contains edges=" + graph, 8, graph.size());
    }

    @Test
    public void testMakeGraphCircle() {
        Join tupleExpr = new Join(new Join(new Join(new Join(sp("ABC"), sp("BCD")), sp("DEF")), sp("FGH")), sp("HIA"));
        HashMultimap<String, TupleExpr> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);

        HashMultimap<TupleExpr, TupleExpr> graph = QueryUnCartesianProductOptimizer.makeGraph(mm);
        printGraph(graph);
        assertEquals("contains nodes=" + graph, 5, graph.keySet().size());
        assertEquals("contains edges=" + graph, 10, graph.size());
    }

    @Test
    public void testMakeGraphFullyConnected() {
        Join tupleExpr = new Join(new Join(new Join(new Join(sp("ABC"), sp("BBB")), sp("DBE")), sp("EFB")), sp("BGB"));
        HashMultimap<String, TupleExpr> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);

        HashMultimap<TupleExpr, TupleExpr> graph = QueryUnCartesianProductOptimizer.makeGraph(mm);
        printGraph(graph);
        assertEquals("contains nodes=" + graph, 5, graph.keySet().size());
        assertEquals("contains edges=" + graph, 20, graph.size());
    }

    /**
     * Prints a graph in a format that can be rendered by Gephi, for example:
     *  *Vertices 6
     *  1 "0"
     *  2 "1"
     *  3 "label"
     *  *Arcs
     *  1 2
     *  2 3
     *  3 1
     * 
     * @param graph is a HashMultimap where nodes map to other notes.
     */
    public static void printGraph(HashMultimap<TupleExpr, TupleExpr> graph) {
        System.out.println("=========begin graph.net==========\n");
        // Got to have an integer nodeid starting with one.
        int nextNodeId = 1;
        // Store map from sp to nodeID
        Map<TupleExpr, Integer> spToId = new HashMap<TupleExpr, Integer>();
        // display all vertices:
        System.out.println("*Vertices " + graph.keySet().size());
        for (TupleExpr sp : graph.keySet()) {
            String nodeName = nodeName(sp);
            int nodeId = nextNodeId;
            nextNodeId++;
            spToId.put(sp, nodeId);
            System.out.println(nodeId + "   " + nodeName);
        }
        // display all edges and weights:
        System.out.println("*Arcs");
        for (TupleExpr sp1 : graph.keySet()) {
            for (TupleExpr sp2 : graph.get(sp1))
                System.out.println(spToId.get(sp1) + "   " + spToId.get(sp2) /* Weight here */);
        }
        System.out.println("\n=========end graph.net==========");

        // json from http://visualgo.net/mst
        // {"vl":{"0":{"x":80,"y":40},"1":{"x":460,"y":200},"2":{"x":160,"y":220},"3":{"x":360,"y":60}},"el":{"0":{"u":0,"v":3,"w":3},"1":{"v":1,"u":3,"w":2},"2":{"u":1,"v":2,"w":4},"3":{"v":0,"u":2,"w":2}}}
    }

    /**
     * Used to generate a tree, concatenates the vars to give a nice node label.
     * 
     * @param sp
     * @param nodeName
     * @return
     */
    private static String nodeName(TupleExpr sp) {
        StringBuilder nodeName = new StringBuilder();
        if (sp instanceof StatementPattern)
            for (Var var : ((StatementPattern) sp).getVarList()) {
                if (nodeName.length() > 0)
                    nodeName.append("-");
                nodeName.append(var.getName());
            }
        else
            for (String var : sp.getBindingNames()) {
                if (nodeName.length() > 0)
                    nodeName.append("-");
                nodeName.append(var);
            }
        return nodeName.toString();
    }
}
