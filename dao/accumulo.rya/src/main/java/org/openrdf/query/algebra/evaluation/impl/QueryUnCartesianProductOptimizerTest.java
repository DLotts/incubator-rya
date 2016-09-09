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
        // Var varA=new Var("A");
        Join tupleExpr = null;
        {
            StatementPattern te1 = sp("ABC");
            StatementPattern te2 = sp("BCD");
            Join join1 = new Join(te1,te2); 
            te2 = sp("DEF");
            Join join2 = new Join(join1,te2); 
            join1 = join2; 
            te2 = sp("FGH");
            join2 = new Join(join1,te2); 
            join1 = join2; 
            te2 = sp("HIJ");
            join2 = new Join(join1,te2); 
            tupleExpr = join2; 
        }
        HashMultimap<String, StatementPattern> mm = QueryUnCartesianProductOptimizer.getVarBins(tupleExpr, null);
        assertEquals("contains 15, A ... J vars.=" + mm, 15, mm.size());
    }

    /**
     * Make a statementpattern easy.
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
     * Make a statementpattern easy.
     * 
     * @param spo
     *            -- split first three chars as the binding variables.
     * @return a new statementpattern
     */
    StatementPattern sp(String spo) {
        return new StatementPattern(new Var(spo.substring(0, 1)), new Var(spo.substring(1, 2)),
                        new Var(spo.substring(2)));
    }
}
