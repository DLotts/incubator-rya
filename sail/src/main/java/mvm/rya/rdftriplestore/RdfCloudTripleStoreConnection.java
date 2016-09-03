package mvm.rya.rdftriplestore;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */



import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static mvm.rya.api.RdfCloudTripleStoreConstants.RANGE;

import info.aduna.iteration.CloseableIteration;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RangeValue;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RdfEvalStatsDAO;
import mvm.rya.api.persist.RyaDAO;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.joinselect.SelectivityEvalDAO;
import mvm.rya.api.persist.utils.RyaDAOHelper;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.rdftriplestore.evaluation.FilterRangeVisitor;
import mvm.rya.rdftriplestore.evaluation.ParallelEvaluationStrategyImpl;
import mvm.rya.rdftriplestore.evaluation.QueryJoinSelectOptimizer;
import mvm.rya.rdftriplestore.evaluation.RdfCloudTripleStoreEvaluationStatistics;
import mvm.rya.rdftriplestore.evaluation.RdfCloudTripleStoreSelectivityEvaluationStatistics;
import mvm.rya.rdftriplestore.evaluation.SeparateFilterJoinsVisitor;
import mvm.rya.rdftriplestore.inference.InferenceEngine;
import mvm.rya.rdftriplestore.inference.InverseOfVisitor;
import mvm.rya.rdftriplestore.inference.SameAsVisitor;
import mvm.rya.rdftriplestore.inference.SubClassOfVisitor;
import mvm.rya.rdftriplestore.inference.SubPropertyOfVisitor;
import mvm.rya.rdftriplestore.inference.SymmetricPropertyVisitor;
import mvm.rya.rdftriplestore.inference.TransitivePropertyVisitor;
import mvm.rya.rdftriplestore.namespace.NamespaceManager;
import mvm.rya.rdftriplestore.provenance.ProvenanceCollectionException;
import mvm.rya.rdftriplestore.provenance.ProvenanceCollector;
import mvm.rya.rdftriplestore.utils.DefaultStatistics;

import org.apache.hadoop.conf.Configurable;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.BinaryTupleOperator;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.BindingAssigner;
import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.ConstantOptimizer;
import org.openrdf.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStatistics;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.openrdf.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailConnectionBase;

import com.google.common.base.Strings;

public class RdfCloudTripleStoreConnection extends SailConnectionBase {

    private RdfCloudTripleStore store;

    private RdfEvalStatsDAO rdfEvalStatsDAO;
    private SelectivityEvalDAO selectEvalDAO;
    private RyaDAO ryaDAO;
    private InferenceEngine inferenceEngine;
    private NamespaceManager namespaceManager;
    private RdfCloudTripleStoreConfiguration conf;
    

	private ProvenanceCollector provenanceCollector;

    public RdfCloudTripleStoreConnection(RdfCloudTripleStore sailBase, RdfCloudTripleStoreConfiguration conf, ValueFactory vf)
            throws SailException {
        super(sailBase);
        this.store = sailBase;
        this.conf = conf;
        initialize();
    }

    protected void initialize() throws SailException {
        refreshConnection();
    }

    protected void refreshConnection() throws SailException {
        try {
            checkNotNull(store.getRyaDAO());
            checkArgument(store.getRyaDAO().isInitialized());
            checkNotNull(store.getNamespaceManager());

            this.ryaDAO = store.getRyaDAO();
            this.rdfEvalStatsDAO = store.getRdfEvalStatsDAO();
            this.selectEvalDAO = store.getSelectEvalDAO();
            this.inferenceEngine = store.getInferenceEngine();
            this.namespaceManager = store.getNamespaceManager();
            this.provenanceCollector = store.getProvenanceCollector();

        } catch (Exception e) {
            throw new SailException(e);
        }
    }

    @Override
    protected void addStatementInternal(Resource subject, URI predicate,
                                        Value object, Resource... contexts) throws SailException {
        try {
            String cv_s = conf.getCv();
            byte[] cv = cv_s == null ? null : cv_s.getBytes();
            if (contexts != null && contexts.length > 0) {
                for (Resource context : contexts) {
                    RyaStatement statement = new RyaStatement(
                            RdfToRyaConversions.convertResource(subject),
                            RdfToRyaConversions.convertURI(predicate),
                            RdfToRyaConversions.convertValue(object),
                            RdfToRyaConversions.convertResource(context),
                            null, cv);

                    ryaDAO.add(statement);
                }
            } else {
                RyaStatement statement = new RyaStatement(
                        RdfToRyaConversions.convertResource(subject),
                        RdfToRyaConversions.convertURI(predicate),
                        RdfToRyaConversions.convertValue(object),
                        null, null, cv);

                ryaDAO.add(statement);
            }
        } catch (RyaDAOException e) {
            throw new SailException(e);
        }
    }

    
    
    
    @Override
    protected void clearInternal(Resource... aresource) throws SailException {
        try {
            RyaURI[] graphs = new RyaURI[aresource.length];
            for (int i = 0 ; i < graphs.length ; i++){
                graphs[i] = RdfToRyaConversions.convertResource(aresource[i]);
            }
            ryaDAO.dropGraph(conf, graphs);
        } catch (RyaDAOException e) {
            throw new SailException(e);
        }
    }

    @Override
    protected void clearNamespacesInternal() throws SailException {
        logger.error("Clear Namespace Repository method not implemented");
    }

    @Override
    protected void closeInternal() throws SailException {
        verifyIsOpen();
    }

    @Override
    protected void commitInternal() throws SailException {
        verifyIsOpen();
        //There is no transactional layer
    }

    @Override
    protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(
            TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
            boolean flag) throws SailException {
        verifyIsOpen();
        logger.trace("Incoming query model:\n{}", tupleExpr.toString());
        if (provenanceCollector != null){
        	try {
				provenanceCollector.recordQuery(tupleExpr.toString());
			} catch (ProvenanceCollectionException e) {
				// TODO silent fail
				e.printStackTrace();
			}
        }
        tupleExpr = tupleExpr.clone();
        reportCrossProduct(tupleExpr,0);
        RdfCloudTripleStoreConfiguration queryConf = store.getConf().clone();
        if (bindings != null) {
            Binding dispPlan = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_QUERYPLAN_FLAG);
            if (dispPlan != null) {
                queryConf.setDisplayQueryPlan(Boolean.parseBoolean(dispPlan.getValue().stringValue()));
            }

            Binding authBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH);
            if (authBinding != null) {
                queryConf.setAuths(authBinding.getValue().stringValue().split(","));
            }

            Binding ttlBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_TTL);
            if (ttlBinding != null) {
                queryConf.setTtl(Long.valueOf(ttlBinding.getValue().stringValue()));
            }

            Binding startTimeBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_STARTTIME);
            if (startTimeBinding != null) {
                queryConf.setStartTime(Long.valueOf(startTimeBinding.getValue().stringValue()));
            }

            Binding performantBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_PERFORMANT);
            if (performantBinding != null) {
                queryConf.setBoolean(RdfCloudTripleStoreConfiguration.CONF_PERFORMANT, Boolean.parseBoolean(performantBinding.getValue().stringValue()));
            }

            Binding inferBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_INFER);
            if (inferBinding != null) {
                queryConf.setInfer(Boolean.parseBoolean(inferBinding.getValue().stringValue()));
            }

            Binding useStatsBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_USE_STATS);
            if (useStatsBinding != null) {
                queryConf.setUseStats(Boolean.parseBoolean(useStatsBinding.getValue().stringValue()));
            }

            Binding offsetBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_OFFSET);
            if (offsetBinding != null) {
                queryConf.setOffset(Long.parseLong(offsetBinding.getValue().stringValue()));
            }

            Binding limitBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_LIMIT);
            if (limitBinding != null) {
                queryConf.setLimit(Long.parseLong(limitBinding.getValue().stringValue()));
            }
        } else {
            bindings = new QueryBindingSet();
        }

        if (!(tupleExpr instanceof QueryRoot)) {
            tupleExpr = new QueryRoot(tupleExpr);
        }

        try {
            List<Class<QueryOptimizer>> optimizers = queryConf.getOptimizers();
            Class<QueryOptimizer> pcjOptimizer = queryConf.getPcjOptimizer();
            
            if(pcjOptimizer != null) {
                QueryOptimizer opt = null;
                try {
                    Constructor<QueryOptimizer> construct = pcjOptimizer.getDeclaredConstructor(new Class[] {});
                    opt = construct.newInstance();
                } catch (Exception e) {
                }
                if (opt == null) {
                    throw new NoSuchMethodException("Could not find valid constructor for " + pcjOptimizer.getName());
                }
                if (opt instanceof Configurable) {
                    ((Configurable) opt).setConf(conf);
                }
                opt.optimize(tupleExpr, dataset, bindings);
            }
            
            final ParallelEvaluationStrategyImpl strategy = new ParallelEvaluationStrategyImpl(
                    new StoreTripleSource(queryConf), inferenceEngine, dataset, queryConf);
            
                (new BindingAssigner()).optimize(tupleExpr, dataset, bindings);
                reportCrossProduct(tupleExpr,0);

                (new ConstantOptimizer(strategy)).optimize(tupleExpr, dataset, bindings);
                reportCrossProduct(tupleExpr,0);
                
                (new CompareOptimizer()).optimize(tupleExpr, dataset, bindings);
                reportCrossProduct(tupleExpr,0);

                (new ConjunctiveConstraintSplitter()).optimize(tupleExpr, dataset,bindings);
                reportCrossProduct(tupleExpr,0);

                (new DisjunctiveConstraintOptimizer()).optimize(tupleExpr, dataset,bindings);
                reportCrossProduct(tupleExpr,0);

                (new SameTermFilterOptimizer()).optimize(tupleExpr, dataset,bindings);
                reportCrossProduct(tupleExpr,0);

                (new QueryModelNormalizer()).optimize(tupleExpr, dataset, bindings);
                reportCrossProduct(tupleExpr,0);

    
                (new IterativeEvaluationOptimizer()).optimize(tupleExpr, dataset,bindings);
                reportCrossProduct(tupleExpr,0);


            for (Class<QueryOptimizer> optclz : optimizers) {
                QueryOptimizer result = null;
                try {
                    Constructor<QueryOptimizer> meth = optclz.getDeclaredConstructor(new Class[] {});
                    result = meth.newInstance();
                } catch (Exception e) {
                }
                if (result == null) {
                    try {
                        Constructor<QueryOptimizer> meth = optclz.getDeclaredConstructor(EvaluationStrategy.class);
                        result = meth.newInstance(strategy);
                    } catch (Exception e) {
                    }
                }
                if (result == null) {
                    throw new NoSuchMethodException("Could not find valid constructor for " + optclz.getName());
                }
                if (result instanceof Configurable) {
                    ((Configurable) result).setConf(conf);
                }
                result.optimize(tupleExpr, dataset, bindings);
            }

            (new FilterOptimizer()).optimize(tupleExpr, dataset, bindings);
            System.out.println("Check crossproduct after FilterOptimizer.");reportCrossProduct(tupleExpr,0);
            
            (new OrderLimitOptimizer()).optimize(tupleExpr, dataset, bindings);
            System.out.println("Check crossproduct after OrderLimitOptimizer.");reportCrossProduct(tupleExpr,0);

            
            logger.trace("Optimized query model:\n{}", tupleExpr.toString());

            if (queryConf.isInfer()
                    && this.inferenceEngine != null
                    ) {
                try {
                    tupleExpr.visit(new TransitivePropertyVisitor(queryConf, inferenceEngine));
                    tupleExpr.visit(new SymmetricPropertyVisitor(queryConf, inferenceEngine));
                    tupleExpr.visit(new InverseOfVisitor(queryConf, inferenceEngine));
                    tupleExpr.visit(new SubPropertyOfVisitor(queryConf, inferenceEngine));
                    tupleExpr.visit(new SubClassOfVisitor(queryConf, inferenceEngine));
                    tupleExpr.visit(new SameAsVisitor(queryConf, inferenceEngine));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("Check crossproduct after inferenceEngine.");reportCrossProduct(tupleExpr,0);
            }
            if (queryConf.isPerformant()) {
                tupleExpr.visit(new SeparateFilterJoinsVisitor());
//                tupleExpr.visit(new FilterTimeIndexVisitor(queryConf));
//                tupleExpr.visit(new PartitionFilterTimeIndexVisitor(queryConf));
                System.out.println("Check crossproduct after isPerformant SeparateFilterJoinsVisitor.");
                reportCrossProduct(tupleExpr,0);
            }
            FilterRangeVisitor rangeVisitor = new FilterRangeVisitor(queryConf);
            tupleExpr.visit(rangeVisitor);
            System.out.println("Check crossproduct after rangeVistor 1.");
            reportCrossProduct(tupleExpr,0);

            tupleExpr.visit(rangeVisitor); //this has to be done twice to get replace the statementpatterns with the right ranges
            System.out.println("Check crossproduct after rangeVistor 2.");
            reportCrossProduct(tupleExpr,0);

            
            EvaluationStatistics stats = null;
            if (!queryConf.isUseStats() && queryConf.isPerformant() || rdfEvalStatsDAO == null) {
                stats = new DefaultStatistics();
            } else if (queryConf.isUseStats()) {

                if (queryConf.isUseSelectivity()) {
                    stats = new RdfCloudTripleStoreSelectivityEvaluationStatistics(queryConf, rdfEvalStatsDAO,
                            selectEvalDAO);
                } else {
                    stats = new RdfCloudTripleStoreEvaluationStatistics(queryConf, rdfEvalStatsDAO);
                }
            }

            if (stats != null) {
                if (stats instanceof RdfCloudTripleStoreSelectivityEvaluationStatistics) {
                    (new QueryJoinSelectOptimizer((RdfCloudTripleStoreSelectivityEvaluationStatistics) stats, selectEvalDAO)).optimize(tupleExpr, dataset, bindings);
                    System.out.println("Check crossproduct after QueryJoinSelectOptimizer.");
                    reportCrossProduct(tupleExpr,0);
                } else {
                    (new mvm.rya.rdftriplestore.evaluation.QueryJoinOptimizer(stats)).optimize(tupleExpr, dataset, bindings); // TODO: Make pluggable
                    System.out.println("Check crossproduct after QueryJoinOptimizer.");
                    reportCrossProduct(tupleExpr,0);
                }
            }

            final CloseableIteration<BindingSet, QueryEvaluationException> iter = strategy
                    .evaluate(tupleExpr, EmptyBindingSet.getInstance());
            CloseableIteration<BindingSet, QueryEvaluationException> iterWrap = new CloseableIteration<BindingSet, QueryEvaluationException>() {
                
                @Override
                public void remove() throws QueryEvaluationException {
                  iter.remove();
                }
                
                @Override
                public BindingSet next() throws QueryEvaluationException {
                    return iter.next();
                }
                
                @Override
                public boolean hasNext() throws QueryEvaluationException {
                    return iter.hasNext();
                }
                
                @Override
                public void close() throws QueryEvaluationException {
                    iter.close();
                    strategy.shutdown();
                }
            };
            return iterWrap;
        } catch (QueryEvaluationException e) {
            throw new SailException(e);
        } catch (Exception e) {
            throw new SailException(e);
        }
    }


    /**
     * Report if the query plan has Cross product issues.
     * This is the non-visitor version. 
     * @param tupleExpr root of the query model
     * @param depth current recursive depth, start with 0.
     */
    public static void reportCrossProductOld(TupleExpr tupleExpr, int depth) {
        depth++;
        if (tupleExpr instanceof BinaryTupleOperator) {
            BinaryTupleOperator join = (BinaryTupleOperator)tupleExpr;

            reportCrossProduct(join.getLeftArg(), depth);
            reportCrossProduct(join.getRightArg(), depth);

            Set<String> intersection;
            Set<String> leftBindings = join.getLeftArg().getBindingNames();
            Set<String> rightBindings = join.getRightArg().getBindingNames();
            intersection = intersection(leftBindings, rightBindings);
            if (intersection.isEmpty()) {
                reportNode(tupleExpr, depth, "\n!!!Crossproduct here!! No common binding names:\n+++leftBindings="+leftBindings+"\n+++rightBindings="+rightBindings);
            } else {
                reportNode(tupleExpr, depth, "\n"+indent(depth)+intersection+" are common.");
            }
        }
        else if (tupleExpr instanceof UnaryTupleOperator) {
            reportCrossProduct(((UnaryTupleOperator)tupleExpr).getArg(), depth-1);
            reportNode(tupleExpr, depth, "");
        } else {
            // leaf node, no children.
            reportNode(tupleExpr, depth, "");
        }
    }

    private static void reportNode(TupleExpr tupleExpr, int depth, String message) {
        System.out.println(indent(depth) 
                + tupleExpr.getClass().getSimpleName() //
                + "    " //
                + tupleExpr.getBindingNames().toString().replaceAll("http.*?(?=[]#,])", "...") //
                + message);
    }
    /**
     * Report if the query plan has Cross product issues.
     * This is the visitor pattern version. 
     * @param tupleExpr root of the query model
     * @param depth current recursive depth, start with 0.  -- not used
     */
    private static void reportCrossProduct(TupleExpr tupleExpr,  int depth) {
        tupleExpr.visit(new CrossProductVisitor()); 
    }

    private static class CrossProductVisitor extends QueryModelVisitorBase<RuntimeException> {
//        public CrossProductVisitor() {
//        }
        private int depth=0; // this will fail if object is shared between threads.
        @Override
        public void meetBinaryTupleOperator(BinaryTupleOperator tupleExpr) throws RuntimeException {
            depth++;
            BinaryTupleOperator join = (BinaryTupleOperator)tupleExpr;
            join.getLeftArg().visit(this);
            join.getRightArg().visit(this);
            Set<String> intersection;
            Set<String> leftBindings = join.getLeftArg().getBindingNames();
            Set<String> rightBindings = join.getRightArg().getBindingNames();
            intersection = intersection(leftBindings, rightBindings);
            if (intersection.isEmpty()) {
                reportNode(tupleExpr, depth, "\n!!!Crossproduct here!! No common bindings:\n+++leftBindings="+leftBindings+"\n+++rightBindings="+rightBindings);
            } else {
                reportNode(tupleExpr, depth, "\n"+indent(depth)+intersection+" are common.");
            }
            depth--;
        }

        @Override
        public void meetUnaryTupleOperator(UnaryTupleOperator node) throws RuntimeException {
            node.getArg().visit(this);
            reportNode(node, depth, "");
        }
        @Override
        public void meetOther(QueryModelNode tupleExpr) throws RuntimeException {
            if (tupleExpr instanceof TupleExpr)
                reportNode((TupleExpr)tupleExpr, depth, "");
            else 
                System.out.println(indent(depth) + tupleExpr.getClass().getSimpleName() ); 
        }
    }
    /**
     * @param level
     * @return
     */
    private static String indent(int level) {
        return "n"+Strings.repeat(".   ", level);
    }

    /**
     * @param leftBindings
     * @param rightBindings
     * @return
     */
    private static Set<String> intersection(Set<String> leftBindings, Set<String> rightBindings) {
        Set<String> intersection;
        {
            intersection = new LinkedHashSet<String>(leftBindings);
            intersection.retainAll(rightBindings);
        }
        return intersection;
    }

    @Override
    protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal()
            throws SailException {
        verifyIsOpen();

        // iterate through all contextids
        return null;
    }

    @Override
    protected String getNamespaceInternal(String s) throws SailException {
        return namespaceManager.getNamespace(s);
    }

    @Override
    protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal()
            throws SailException {
        return namespaceManager.iterateNamespace();
    }

    @Override
    protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(
            Resource subject, URI predicate, Value object, boolean flag,
            Resource... contexts) throws SailException {
//        try {
        //have to do this to get the inferred values
        //TODO: Will this method reduce performance?
        final Var subjVar = decorateValue(subject, "s");
        final Var predVar = decorateValue(predicate, "p");
        final Var objVar = decorateValue(object, "o");
        StatementPattern sp = null;
        final boolean hasContext = contexts != null && contexts.length > 0;
        final Resource context = (hasContext) ? contexts[0] : null;
        final Var cntxtVar = decorateValue(context, "c");
        //TODO: Only using one context here
        sp = new StatementPattern(subjVar, predVar, objVar, cntxtVar);
        //return new StoreTripleSource(store.getConf()).getStatements(resource, uri, value, contexts);
        final CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate = evaluate(sp, null, null, false);
        return new CloseableIteration<Statement, SailException>() {  //TODO: Use a util class to do this
            private boolean isClosed = false;

            @Override
            public void close() throws SailException {
            isClosed = true;
                try {
                    evaluate.close();
                } catch (QueryEvaluationException e) {
                    throw new SailException(e);
                }
            }

            @Override
            public boolean hasNext() throws SailException {
                try {
                    return evaluate.hasNext();
                } catch (QueryEvaluationException e) {
                    throw new SailException(e);
                }
            }

            @Override
            public Statement next() throws SailException {
                if (!hasNext() || isClosed) {
                    throw new NoSuchElementException();
                }

                try {
                    BindingSet next = evaluate.next();
                    Resource bs_subj = (Resource) ((subjVar.hasValue()) ? subjVar.getValue() : next.getBinding(subjVar.getName()).getValue());
                    URI bs_pred = (URI) ((predVar.hasValue()) ? predVar.getValue() : next.getBinding(predVar.getName()).getValue());
                    Value bs_obj = (objVar.hasValue()) ? objVar.getValue() : (Value) next.getBinding(objVar.getName()).getValue();
                    Binding b_cntxt = next.getBinding(cntxtVar.getName());

                    //convert BindingSet to Statement
                    if (b_cntxt != null) {
                        return new ContextStatementImpl(bs_subj, bs_pred, bs_obj, (Resource) b_cntxt.getValue());
                    } else {
                        return new StatementImpl(bs_subj, bs_pred, bs_obj);
                    }
                } catch (QueryEvaluationException e) {
                    throw new SailException(e);
                }
            }

            @Override
            public void remove() throws SailException {
                try {
                    evaluate.remove();
                } catch (QueryEvaluationException e) {
                    throw new SailException(e);
                }
            }
        };
//        } catch (QueryEvaluationException e) {
//            throw new SailException(e);
//        }
    }

    protected Var decorateValue(Value val, String name) {
        if (val == null) {
            return new Var(name);
        } else {
            return new Var(name, val);
        }
    }

    @Override
    protected void removeNamespaceInternal(String s) throws SailException {
        namespaceManager.removeNamespace(s);
    }

    @Override
    protected void removeStatementsInternal(Resource subject, URI predicate,
                                            Value object, Resource... contexts) throws SailException {
        if (!(subject instanceof URI)) {
            throw new SailException("Subject[" + subject + "] must be URI");
        }

        try {
            if (contexts != null && contexts.length > 0) {
                for (Resource context : contexts) {
                    if (!(context instanceof URI)) {
                        throw new SailException("Context[" + context + "] must be URI");
                    }
                    RyaStatement statement = new RyaStatement(
                            RdfToRyaConversions.convertResource(subject),
                            RdfToRyaConversions.convertURI(predicate),
                            RdfToRyaConversions.convertValue(object),
                            RdfToRyaConversions.convertResource(context));

                    ryaDAO.delete(statement, conf);
                }
            } else {
                RyaStatement statement = new RyaStatement(
                        RdfToRyaConversions.convertResource(subject),
                        RdfToRyaConversions.convertURI(predicate),
                        RdfToRyaConversions.convertValue(object),
                        null);

                ryaDAO.delete(statement, conf);
            }
        } catch (RyaDAOException e) {
            throw new SailException(e);
        }
    }

    @Override
    protected void rollbackInternal() throws SailException {
        //TODO: No transactional layer as of yet
    }

    @Override
    protected void setNamespaceInternal(String s, String s1)
            throws SailException {
        namespaceManager.addNamespace(s, s1);
    }

    @Override
    protected long sizeInternal(Resource... contexts) throws SailException {
        logger.error("Cannot determine size as of yet");

        return 0;
    }

    @Override
    protected void startTransactionInternal() throws SailException {
        //TODO: ?
    }

    public class StoreTripleSource implements TripleSource {

        private RdfCloudTripleStoreConfiguration conf;

        public StoreTripleSource(RdfCloudTripleStoreConfiguration conf) {
            this.conf = conf;
        }

        public CloseableIteration<Statement, QueryEvaluationException> getStatements(
                Resource subject, URI predicate, Value object,
                Resource... contexts) throws QueryEvaluationException {
            return RyaDAOHelper.query(ryaDAO, subject, predicate, object, conf, contexts);
        }

        public CloseableIteration<? extends Entry<Statement, BindingSet>, QueryEvaluationException> getStatements(
                Collection<Map.Entry<Statement, BindingSet>> statements,
                Resource... contexts) throws QueryEvaluationException {

            return RyaDAOHelper.query(ryaDAO, statements, conf);
        }

        public ValueFactory getValueFactory() {
            return RdfCloudTripleStoreConstants.VALUE_FACTORY;
        }
    }
    
    public InferenceEngine getInferenceEngine() {
        return inferenceEngine;
    }
    public RdfCloudTripleStoreConfiguration getConf() {
        return conf;
    }
}
