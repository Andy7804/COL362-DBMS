package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class implements the JoinPredicate interface.
 *
 * DO NOT CHANGE the constructor or existing member variables!
 * TODO: Implement the evaluate() method and use it in your implementation of the join operator
 *
 */
public class EqualityJoinPredicate implements JoinPredicate {

    protected final static Logger logger = LogManager.getLogger();

    private final String leftColumn; // left column name
    private final String rightColumn; // right column name

    public EqualityJoinPredicate(String leftColumn, String rightColumn) {
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
    }

    /**
     * Evaluates if left and right tuples.
     * @param left  the tuple from the left input
     * @param right the tuple from the right input
     * @return true if the tuples match based on their leftColumn and rightColumn value
     */
    @Override
    public boolean evaluate(Tuple left, Tuple right) {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Left tuple " + left.getValues() + "[" + left.getSchema() + "]");
        logger.trace("Right tuple " + right.getValues() + "[" + right.getSchema() + "]");
        logger.trace("Condition " + leftColumn + " = " + rightColumn);
        // -------------------------

        //TODO: Implement me!
        int leftColumnIndex = left.getSchema().indexOf(leftColumn);
        int rightColumnIndex = right.getSchema().indexOf(rightColumn);
        if (leftColumnIndex == -1 || rightColumnIndex == -1) {
            throw new IllegalArgumentException("Column not found in schema: " +
                    (leftColumnIndex == -1 ? leftColumn : rightColumn));
        }
        Object leftValue = left.getValues().get(leftColumnIndex);
        Object rightValue = right.getValues().get(rightColumnIndex);

        if(leftValue == null || rightValue == null) return false; //Ignore if any row has null values
        return (customComparator(leftValue, rightValue) == 0);
    }

    private int customComparator(Object left, Object right) {
        if(left == null || right == null) {
            throw new IllegalArgumentException("Null values are not supported.");
        }
        // Numeric comparison
        if (left instanceof Number && right instanceof Number) {
            double leftVal = ((Number) left).doubleValue();
            double rightVal = ((Number) right).doubleValue();
            return Double.compare(leftVal, rightVal);
        }
        // String comparison
        else if (left instanceof String && right instanceof String) {
            return ((String) left).compareTo((String) right);
        }
        else {
            throw new IllegalArgumentException("Type mismatch or unsupported types: " +
                    left.getClass().getSimpleName() + " vs " + right.getClass().getSimpleName());
        }
    }

    // DO NOT REMOVE THESE METHODS
    public String getLeftColumn() {
        return leftColumn;
    }

    public String getRightColumn() {
        return rightColumn;
    }

    @Override
    public String toString() {
        return "EqualityJoinPredicate[" +
                "leftColumn='" + leftColumn + '\'' +
                ", rightColumn='" + rightColumn + '\'' +
                ']';
    }
}
