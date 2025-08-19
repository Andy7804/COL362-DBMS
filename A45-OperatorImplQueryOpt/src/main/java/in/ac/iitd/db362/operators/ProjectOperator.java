package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

import java.util.*;

/**
 * Implementation of a simple project operator that implements the operator interface.
 *
 *
 * TODO: Implement the open(), next(), and close() methods!
 * Do not change the constructor or existing member variables.
 */
public class ProjectOperator extends OperatorBase implements Operator {
    private Operator child;
    private List<String> projectedColumns;
    private boolean distinct;
    private Set<List<Object>> seenProjections;


    /**
     * Project operator. If distinct is set to true, it does duplicate elimination
     * @param child
     * @param projectedColumns
     * @param distinct
     */
    public ProjectOperator(Operator child, List<String> projectedColumns, boolean distinct) {
        this.child = child;
        this.projectedColumns = projectedColumns;
        this.distinct = distinct;
    }

    @Override
    public void open() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Open()");
        // -------------------------

        // TODO: Implement me!
        child.open();
        if(distinct) {
            seenProjections = new HashSet<>();
        }
    }

    @Override
    public Tuple next() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Next()");
        // ------------------------

        //TODO: Implement me!
        Tuple originalTuple;
        // in this case, the while-loop doesn't actually loop, but it will loop later on in case of duplicates
        while ((originalTuple = child.next()) != null) {
            // Project the tuple to the specified columns
            List<Object> projectedValues = new ArrayList<>();
            List<String> projectedSchema = new ArrayList<>();
            for (String column : projectedColumns) {
                projectedValues.add(originalTuple.get(column));
                projectedSchema.add(column);
            }
            // If distinct is false, return immediately
            if (!distinct) {
                return new Tuple(projectedValues, projectedSchema);
            }
            // If distinct, return only if projection is not seen before
            if (seenProjections.add(projectedValues)) { // add returns false if already seen
                return new Tuple(projectedValues, projectedSchema);
            }
        }
        return null;
    }

    @Override
    public void close() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Close()");
        // ------------------------

        // TODO: Implement me!
        child.close();
        seenProjections = null;
    }

    // do not remvoe these methods!
    public Operator getChild() {
        return child;
    }

    public List<String> getProjectedColumns() {
        return projectedColumns;
    }

    public boolean isDistinct() {
        return distinct;
    }
}
