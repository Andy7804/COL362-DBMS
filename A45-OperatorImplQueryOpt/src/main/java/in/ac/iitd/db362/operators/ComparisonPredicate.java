package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Note: ONLY IMPLEMENT THE EVALUATE METHOD.
 * TODO: Implement the evaluate() method
 *
 * DO NOT CHANGE the constructor or existing member variables.
 *
 * A comparison predicate for simple atomic predicates.
 */
public class ComparisonPredicate implements Predicate {

    protected final static Logger logger = LogManager.getLogger();
    private final Object leftOperand;   // Either a constant or a column reference (String)
    private final String operator;        // One of: =, >, >=, <, <=, !=
    private final Object rightOperand;  // Either a constant or a column reference (String)

    public ComparisonPredicate(Object leftOperand, String operator, Object rightOperand) {
        this.leftOperand = leftOperand;
        this.operator = operator;
        this.rightOperand = rightOperand;
    }

    /**
     * Evaluate a tuple
     * @param tuple the tuple to evaluate
     * @return return true if leftOperator operator righOperand holds in that tuple
     */
    @Override
    public boolean evaluate(Tuple tuple) {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Evaluating tuple " + tuple.getValues() + " with schema " + tuple.getSchema());
        logger.trace("[Predicate] " + leftOperand + " " + operator + " " + rightOperand);
        // -------------------------

        //TODO: Implement me!
        //Implementing for now with the assumption that a basic predicate of the form A op c
        //If the column to be compared in predicate is not present in the tuple, ignore
        if (!tuple.getSchema().contains((String) leftOperand)) {
            return true;
        }
        Object leftValue = resolveOperand(leftOperand, tuple);
        // need to handle the right value case after tomorrow's clarification - doubt #1
        Object rightValue = rightOperand; // This will fail if there is a column name with the same value as constant
        switch(operator) {
            case "=" : return (customComparator(leftValue, rightValue) == 0);
            case ">" : return (customComparator(leftValue, rightValue) > 0);
            case ">=": return (customComparator(leftValue, rightValue) >= 0);
            case "<" : return (customComparator(leftValue, rightValue) < 0);
            case "<=": return (customComparator(leftValue, rightValue) <= 0);
            case "!=": return (customComparator(leftValue, rightValue) != 0);
            default: throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }

    private Object resolveOperand(Object operand, Tuple tuple) {
        if (operand instanceof String) {
            String column = (String) operand;
            if (tuple.getSchema().contains(column)) {
                return tuple.get(column); // Column value
            } else {
                return column; // String constant
            }
        }
        return operand; // Non-String constant (e.g., Integer)
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

    // DO NOT REMOVE these functions! ---
    @Override
    public String toString() {
        return "ComparisonPredicate[" +
                "leftOperand=" + leftOperand +
                ", operator='" + operator + '\'' +
                ", rightOperand=" + rightOperand +
                ']';
    }
    public Object getLeftOperand() {
        return leftOperand;
    }

    public String getOperator() {
        return operator;
    }
    public Object getRightOperand() {
        return rightOperand;
    }

}
