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
     * Does not test Caresian Products exists.
     * 
     * @param tupleExpr
     */
    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        tupleExpr.visit(new JoinVisitor());
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
     * put nodes in bins/buckets by there included variable names. a statement
     * pattern with variables A and C goes in A bin and C bin.
     * 
     * @param nodes
     * @return
     */
    @VisibleForTesting
    static HashMultimap<String, StatementPattern> getVarBins(TupleExpr tupleExpr,
                    HashMultimap<String, StatementPattern> varMap) {
        if (varMap == null)
            varMap = HashMultimap.create();
        if (tupleExpr instanceof BinaryTupleOperator) {
            BinaryTupleOperator join = (BinaryTupleOperator) tupleExpr;
            getVarBins(join.getLeftArg(), varMap);
            getVarBins(join.getRightArg(), varMap);
        } else if (tupleExpr instanceof UnaryTupleOperator) {
            getVarBins(((UnaryTupleOperator) tupleExpr).getArg(), varMap);
        } else {
            // leaf node, no children.
            if (tupleExpr instanceof StatementPattern) {
                // Grab the variables, and sort the StatementPattern into bins:
                StatementPattern sp = (StatementPattern) tupleExpr;
                for (Var var : sp.getVarList())
                {
                    if (!var.isConstant()) {
                        varMap.put(var.getName(), sp);
                    }
                }
            }
        }
        return varMap;
    }

}
