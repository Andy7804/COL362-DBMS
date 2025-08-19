package in.ac.iitd.db362.optimizer;

import in.ac.iitd.db362.catalog.Catalog;
import in.ac.iitd.db362.api.PlanPrinter;
import in.ac.iitd.db362.operators.Operator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
// doing a few additional imports to simplify my implementation for now
import in.ac.iitd.db362.catalog.*;
import in.ac.iitd.db362.operators.*;
import java.util.*;

/**
 * A basic optimizer implementation. Feel free and be creative in designing your optimizer.
 * Do not change the constructor. Use the catalog for various statistics that are available.
 * For everything in your optimization logic, you are free to do what ever you want.
 * Make sure to write efficient code!
 */
public class BasicOptimizer implements Optimizer {

    // Do not remove or rename logger
    protected final Logger logger = LogManager.getLogger(this.getClass());

    // Do not remove or rename catalog. You'll need it in your optimizer
    private final Catalog catalog;

    /**
     * DO NOT CHANGE THE CONSTRUCTOR!
     *
     * @param catalog
     */
    public BasicOptimizer(Catalog catalog) {
        this.catalog = catalog;
    }

    /**
     * Basic optimization that currently does not modify the plan. Your goal is to come up with
     * an optimization strategy that should find an optimal plan. Come up with your own ideas or adopt the ones
     * discussed in the lecture to efficiently enumerate plans, a search strategy along with a cost model.
     *
     * @param plan The original query plan.
     * @return The (possibly) optimized query plan.
     */
    @Override
    public Operator optimize(Operator plan) {
        logger.info("Optimizing Plan:\n{}", PlanPrinter.getPlanString(plan));

        // Step 1: Push down filters
        Operator planWithPushedFilters = pushDownFilters(plan);
        logger.info("Plan after filter pushdown:\n{}", PlanPrinter.getPlanString(planWithPushedFilters));
        // Initialize services and data structures here
        StatisticsQueryService statsService = new StatisticsQueryService(catalog);
        Map<Operator, Double> cardinalityEstimates = new HashMap<>();

        Operator optimizedPlan = planWithPushedFilters;

        try {
            // Step 2: Optimize join ordering
            optimizedPlan = optimizeJoins(planWithPushedFilters, statsService, cardinalityEstimates);
            logger.info("Plan after join optimization:\n{}", PlanPrinter.getPlanString(optimizedPlan));
        }
        catch (Exception ignored) {}

        return optimizedPlan;
    }

    /**
     * Pushes filter operators down in the query plan tree,
     * as close to the scan operators as possible.
     *
     * @param op The root operator of the query plan
     * @return The optimized query plan with pushed down filters
     */
    private Operator pushDownFilters(Operator op) {
        // Collect filters from the tree
        List<FilterOperator> filters = new ArrayList<>();
        Operator noFilterPlan = removeFilters(op, filters);

        // Build a mapping from table name to associated filters
        Map<String, List<FilterOperator>> tableFilters = mapFiltersToTables(filters);

        // Apply the filters just above their respective scan operators
        return insertFiltersAboveScans(noFilterPlan, tableFilters);
    }

    /**
     * Removes filter operators from the tree and collects them
     *
     * @param op The current operator to process
     * @param collectedFilters List to store removed filter operators
     * @return The plan without filter operators
     */
    private Operator removeFilters(Operator op, List<FilterOperator> collectedFilters) {
        if (op == null) {
            return null;
        }

        if (op instanceof FilterOperator) {
            FilterOperator filter = (FilterOperator) op;
            collectedFilters.add(filter);
            return removeFilters(filter.getChild(), collectedFilters);
        } else if (op instanceof ProjectOperator) {
            ProjectOperator project = (ProjectOperator) op;
            Operator newChild = removeFilters(project.getChild(), collectedFilters);
            return new ProjectOperator(newChild, project.getProjectedColumns(), project.isDistinct());
        } else if (op instanceof JoinOperator) {
            JoinOperator join = (JoinOperator) op;
            Operator newLeft = removeFilters(join.getLeftChild(), collectedFilters);
            Operator newRight = removeFilters(join.getRightChild(), collectedFilters);
            return new JoinOperator(newLeft, newRight, join.getPredicate());
        } else if (op instanceof SinkOperator) {
            SinkOperator sink = (SinkOperator) op;
            Operator newChild = removeFilters(sink.getChild(), collectedFilters);
            return new SinkOperator(newChild, sink.getOutputFile());
        }

        // ScanOperator or other terminal operators - no modification needed
        return op;
    }

    /**
     * Maps filter operators to their respective tables based on column references
     *
     * @param filters The list of filter operators to map
     * @return Map from table name to list of associated filters
     */
    private Map<String, List<FilterOperator>> mapFiltersToTables(List<FilterOperator> filters) {
        Map<String, List<FilterOperator>> tableFilters = new HashMap<>();

        for (FilterOperator filter : filters) {
            Predicate predicate = filter.getPredicate();
            String tableName = null;

            if (predicate instanceof ComparisonPredicate) {
                ComparisonPredicate compPred = (ComparisonPredicate) predicate;
                Object leftOp = compPred.getLeftOperand();

                if (leftOp instanceof String) {
                    tableName = catalog.getTableForColumn((String) leftOp);
                }
            }

            if (tableName != null) {
                tableFilters.computeIfAbsent(tableName, k -> new ArrayList<>()).add(filter);
            }
        }

        return tableFilters;
    }

    /**
     * Inserts filter operators just above their respective scan operators
     *
     * @param op The current operator to process
     * @param tableFilters Map from table name to associated filters
     * @return The plan with inserted filters
     */
    private Operator insertFiltersAboveScans(Operator op, Map<String, List<FilterOperator>> tableFilters) {
        if (op == null) {
            return null;
        }

        if (op instanceof ScanOperator) {
            ScanOperator scan = (ScanOperator) op;
            String filePath = scan.getFilePath();
            List<FilterOperator> filters = tableFilters.get(filePath);

            if (filters != null && !filters.isEmpty()) {
                // Stack filters above the scan
                Operator current = scan;
                for (FilterOperator filter : filters) {
                    current = new FilterOperator(current, filter.getPredicate());
                }
                return current;
            }
            return scan;
        } else if (op instanceof ProjectOperator) {
            ProjectOperator project = (ProjectOperator) op;
            Operator newChild = insertFiltersAboveScans(project.getChild(), tableFilters);
            return new ProjectOperator(newChild, project.getProjectedColumns(), project.isDistinct());
        } else if (op instanceof JoinOperator) {
            JoinOperator join = (JoinOperator) op;
            Operator newLeft = insertFiltersAboveScans(join.getLeftChild(), tableFilters);
            Operator newRight = insertFiltersAboveScans(join.getRightChild(), tableFilters);
            return new JoinOperator(newLeft, newRight, join.getPredicate());
        } else if (op instanceof SinkOperator) {
            SinkOperator sink = (SinkOperator) op;
            Operator newChild = insertFiltersAboveScans(sink.getChild(), tableFilters);
            return new SinkOperator(newChild, sink.getOutputFile());
        } else if (op instanceof FilterOperator) {
            // This shouldn't happen as we've removed all filters,
            // but handling it just in case
            FilterOperator filter = (FilterOperator) op;
            Operator newChild = insertFiltersAboveScans(filter.getChild(), tableFilters);
            return new FilterOperator(newChild, filter.getPredicate());
        }

        return op;
    }

    /**
     * Optimizes join ordering based on cardinality estimates
     *
     * @param op The root operator of the query plan
     * @param statsService The statistics query service to use
     * @param cardinalityEstimates Map to store cardinality estimates
     * @return The optimized query plan with reordered joins
     */
    private Operator optimizeJoins(Operator op, StatisticsQueryService statsService,
                                   Map<Operator, Double> cardinalityEstimates) {
        // First recursively estimate cardinalities for all operators
        estimateCardinality(op, statsService, cardinalityEstimates);

        // Then recursively optimize join orders
        return reorderJoins(op, statsService, cardinalityEstimates);
    }

    /**
     * Estimates the cardinality of each operator in the tree
     *
     * @param op The operator to process
     * @param statsService The statistics query service to use
     * @param cardinalityEstimates Map to store cardinality estimates
     * @return The estimated cardinality of the operator
     */
    private double estimateCardinality(Operator op, StatisticsQueryService statsService,
                                       Map<Operator, Double> cardinalityEstimates) {
        if (op == null) {
            return 0.0;
        }

        // Check if we've already computed this
        if (cardinalityEstimates.containsKey(op)) {
            return cardinalityEstimates.get(op);
        }

        double estimate;

        if (op instanceof ScanOperator) {
            ScanOperator scan = (ScanOperator) op;
            TableStatistics stats = catalog.getTableStatistics(scan.getFilePath());
            estimate = (stats != null) ? stats.getNumRows() : 1000.0; // Default if no stats
        } else if (op instanceof FilterOperator) {
            FilterOperator filter = (FilterOperator) op;
            double childEstimate = estimateCardinality(filter.getChild(), statsService, cardinalityEstimates);
            double selectivity = estimateSelectivity(filter.getPredicate(), statsService);
            estimate = childEstimate * selectivity;
        } else if (op instanceof ProjectOperator) {
            ProjectOperator project = (ProjectOperator) op;
            double childEstimate = estimateCardinality(project.getChild(), statsService, cardinalityEstimates);
            // If it's a distinct projection, apply a reduction factor
            if (project.isDistinct()) {
                estimate = childEstimate * 0.5; // Simple heuristic for distinct
            } else {
                estimate = childEstimate; // No change for regular projection
            }
        } else if (op instanceof JoinOperator) {
            JoinOperator join = (JoinOperator) op;
            double leftEstimate = estimateCardinality(join.getLeftChild(), statsService, cardinalityEstimates);
            double rightEstimate = estimateCardinality(join.getRightChild(), statsService, cardinalityEstimates);
            double joinSelectivity = estimateJoinSelectivity(join, statsService);
            estimate = leftEstimate * rightEstimate * joinSelectivity;
        } else if (op instanceof SinkOperator) {
            SinkOperator sink = (SinkOperator) op;
            estimate = estimateCardinality(sink.getChild(), statsService, cardinalityEstimates);
        } else {
            // Default for unknown operator types
            estimate = 1000.0;
        }

        // Store and return the estimate
        cardinalityEstimates.put(op, estimate);
        return estimate;
    }

    /**
     * Estimates the selectivity of a filter predicate
     *
     * @param predicate The predicate to estimate
     * @param statsService The statistics query service to use
     * @return The estimated selectivity (between 0 and 1)
     */
    private double estimateSelectivity(Predicate predicate, StatisticsQueryService statsService) {
        if (predicate instanceof ComparisonPredicate) {
            ComparisonPredicate compPred = (ComparisonPredicate) predicate;
            Object leftOp = compPred.getLeftOperand();
            Object rightOp = compPred.getRightOperand();
            String operator = compPred.getOperator();

            if (leftOp instanceof String) {
                String columnName = (String) leftOp;
                String tableName = catalog.getTableForColumn(columnName);

                if (tableName != null) {
                    switch (operator) {
                        case "=":
                            return statsService.getEqualitySelectivity(tableName, columnName, rightOp);
                        case ">":
                        case ">=":
                            return statsService.getRangeSelectivity(tableName, columnName, rightOp, null);
                        case "<":
                        case "<=":
                            return statsService.getRangeSelectivity(tableName, columnName, null, rightOp);
                        case "!=":
                            return 1.0 - statsService.getEqualitySelectivity(tableName, columnName, rightOp);
                        default:
                            return 0.5; // Default for unknown operators
                    }
                }
            }
        }

        return 0.3; // Default selectivity
    }

    /**
     * Estimates the selectivity of a join operation
     *
     * @param join The join operator
     * @param statsService The statistics query service to use
     * @return The estimated join selectivity
     */
    private double estimateJoinSelectivity(JoinOperator join, StatisticsQueryService statsService) {
        if (join.getPredicate() instanceof EqualityJoinPredicate) {
            EqualityJoinPredicate joinPred = (EqualityJoinPredicate) join.getPredicate();
            String leftColumn = joinPred.getLeftColumn();
            String rightColumn = joinPred.getRightColumn();

            String leftTable = catalog.getTableForColumn(leftColumn);
            String rightTable = catalog.getTableForColumn(rightColumn);

            if (leftTable != null && rightTable != null) {
                // Use the inverse of the maximum cardinality as a heuristic
                TableStatistics leftStats = catalog.getTableStatistics(leftTable);
                TableStatistics rightStats = catalog.getTableStatistics(rightTable);

                if (leftStats != null && rightStats != null) {
                    double leftCardinality = leftStats.getColumnStatistics(leftColumn).getCardinality();
                    double rightCardinality = rightStats.getColumnStatistics(rightColumn).getCardinality();
                    double maxCardinality = Math.max(leftCardinality, rightCardinality);

                    return 1.0 / maxCardinality;
                }
            }
        }

        return 0.1; // Default join selectivity
    }

    /**
     * Reorders joins to optimize the query plan
     *
     * @param op The operator to process
     * @param statsService The statistics query service to use
     * @param cardinalityEstimates Map with cardinality estimates
     * @return The optimized operator
     */
    private Operator reorderJoins(Operator op, StatisticsQueryService statsService,
                                  Map<Operator, Double> cardinalityEstimates) {
        if (op == null) {
            return null;
        }

        if (op instanceof JoinOperator) {
            JoinOperator join = (JoinOperator) op;

            // Recursively optimize subtrees first
            Operator optimizedLeft = reorderJoins(join.getLeftChild(), statsService, cardinalityEstimates);
            Operator optimizedRight = reorderJoins(join.getRightChild(), statsService, cardinalityEstimates);

            // Get cardinality estimates for both children
            double leftEstimate = cardinalityEstimates.getOrDefault(optimizedLeft,
                    estimateCardinality(optimizedLeft, statsService, cardinalityEstimates));
            double rightEstimate = cardinalityEstimates.getOrDefault(optimizedRight,
                    estimateCardinality(optimizedRight, statsService, cardinalityEstimates));

            // Put the smaller cardinality side on the left (for hash join)
            if (rightEstimate < leftEstimate) {
                // Swap sides if right side has smaller cardinality
                // Note: This assumes joins are commutative (e.g., equality joins)
                if (join.getPredicate() instanceof EqualityJoinPredicate) {
                    EqualityJoinPredicate predicate = (EqualityJoinPredicate) join.getPredicate();
                    // Create a new predicate with swapped columns
                    EqualityJoinPredicate swappedPredicate =
                            new EqualityJoinPredicate(predicate.getRightColumn(), predicate.getLeftColumn());

                    return new JoinOperator(optimizedRight, optimizedLeft, swappedPredicate);
                }
            }

            return new JoinOperator(optimizedLeft, optimizedRight, join.getPredicate());

        } else if (op instanceof FilterOperator) {
            FilterOperator filter = (FilterOperator) op;
            Operator optimizedChild = reorderJoins(filter.getChild(), statsService, cardinalityEstimates);
            return new FilterOperator(optimizedChild, filter.getPredicate());
        } else if (op instanceof ProjectOperator) {
            ProjectOperator project = (ProjectOperator) op;
            Operator optimizedChild = reorderJoins(project.getChild(), statsService, cardinalityEstimates);
            return new ProjectOperator(optimizedChild, project.getProjectedColumns(), project.isDistinct());
        } else if (op instanceof SinkOperator) {
            SinkOperator sink = (SinkOperator) op;
            Operator optimizedChild = reorderJoins(sink.getChild(), statsService, cardinalityEstimates);
            return new SinkOperator(optimizedChild, sink.getOutputFile());
        }

        // ScanOperator or other terminal operators - no modification needed
        return op;
    }
}
