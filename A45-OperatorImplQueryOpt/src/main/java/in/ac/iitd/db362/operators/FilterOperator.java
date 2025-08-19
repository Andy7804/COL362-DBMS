package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

/**
 * The filter operator produces tuples that satisfy the predicate
 * TODO: Implement the open(), next(), and close() methods.
 *
 * Do not change the constructor and member variables or getters
 * Do not remove logging! otherwise your test cases will fail!
 */
public class FilterOperator extends OperatorBase implements Operator {
    private Operator child;
    private Predicate predicate;

    public FilterOperator(Operator child, Predicate predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    @Override
    public void open() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Open()");
        // ------------------------

        //TODO: Implement me!
        //Error handling not being done as of now, need to understand the pipeline better for what kind of errors may creep in
        child.open();
    }

    @Override
    public Tuple next() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Next()");
        // -------------------------

        //TODO: Implement me!
        //Error handling not being done as of now, need to understand the pipeline better for what kind of errors may creep in
        Tuple tuple;
        while ((tuple = child.next()) != null) {
            if (predicate.evaluate(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public void close() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Close()");
        // -------------------------

        //TODO: Implement me!
        //Error handling not being done as of now, need to understand the pipeline better for what kind of errors may creep in
        child.close();
    }


    // Do not remove these methods!
    public Operator getChild() {
        return child;
    }

    public Predicate getPredicate() {
        return predicate;
    }
}
