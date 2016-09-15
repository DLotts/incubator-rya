/* 
 * Licensed to Aduna under one or more contributor license agreements.  
 * See the NOTICE.txt file distributed with this work for additional 
 * information regarding copyright ownership. 
 *
 * Aduna licenses this file to you under the terms of the Aduna BSD 
 * License (the "License"); you may not use this file except in compliance 
 * with the License. See the LICENSE.txt file distributed with this work 
 * for the full License.
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.openrdf.query.algebra.evaluation.impl;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.BinaryTupleOperator;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;

/**
 * A query optimizer that repairs Joins orders that give Caresian Products.
 * Caresian Products are produced by two statements that have no variable names
 * in common.
 */
public class QueryUnCartesianProductOptimizer implements QueryOptimizer {
    /**
     * Does not test Cartesian Products exists.
     * 
     * @param tupleExpr
     */
    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        HashMultimap<String, TupleExpr> varMap = getVarBins(tupleExpr, null);
        HashMultimap<TupleExpr, TupleExpr> graph = makeGraph(varMap);
        QueryUnCartesianProductOptimizerTest.printGraph(graph);
        // TODO rearrange the query:
        // tupleExpr.visit(new JoinVisitor());
    }

    protected class JoinVisitor extends QueryModelVisitorBase<RuntimeException> {
        @Override
        public void meet(LeftJoin leftJoin) {
        }

        @Override
        public void meet(Join node) {
        }
    }

    /**
     * put nodes in bins/buckets by their included variable names. a statement
     * pattern with variables A and C goes in A bin and C bin.
     * 
     * @param nodes
     * @return
     */
    @VisibleForTesting
    static HashMultimap<String, TupleExpr> getVarBins(TupleExpr tupleExpr, HashMultimap<String, TupleExpr> varMap) {
        if (varMap == null)
            varMap = HashMultimap.create();
        if (tupleExpr instanceof BinaryTupleOperator) {
            if (tupleExpr instanceof Join) {
                BinaryTupleOperator join = (BinaryTupleOperator) tupleExpr;
                getVarBins(join.getLeftArg(), varMap);
                getVarBins(join.getRightArg(), varMap);
            } else {
                // treat non-joins as similar to statement patterns, don't
                // traverse them.
                // particularly Unions and LeftJoins that can't be disassembled
                // arbitrarily.
                // TODO Remember this node and optimize this subtree later.
                // Grab the variables, and sort the StatementPattern into bins:
                for (String bindingName : tupleExpr.getBindingNames()) {
                    Var var = new Var(bindingName);
                    if (!var.isConstant()) {
                        varMap.put(var.getName(), tupleExpr);
                    }
                }
            }
        } else if (tupleExpr instanceof UnaryTupleOperator) {
            getVarBins(((UnaryTupleOperator) tupleExpr).getArg(), varMap);
        } else {
            // leaf node, no children.
            if (tupleExpr instanceof StatementPattern) {
                // Grab the variables, and sort the StatementPattern into bins:
                StatementPattern sp = (StatementPattern) tupleExpr;
                for (Var var : sp.getVarList()) {
                    if (!var.isConstant()) {
                        varMap.put(var.getName(), sp);
                    }
                }
            } else {
                System.out.println("Some unknown leaf node, ignoring:" + tupleExpr);
            }
        }
        return varMap;
    }

    @VisibleForTesting
    static HashMultimap<TupleExpr, TupleExpr> makeGraph(HashMultimap<String, TupleExpr> varIndexToSp) {
        HashMultimap<TupleExpr, TupleExpr> graph = HashMultimap.create();
        for (String var : varIndexToSp.keySet())
            for (TupleExpr sp1 : varIndexToSp.get(var)) {
                for (TupleExpr sp2 : varIndexToSp.get(var)) {
                    if (sp1 != sp2)
                        // Add edges in both ways:
                        graph.put(sp1, sp2);
                }
            }
        return graph;
    }
}
