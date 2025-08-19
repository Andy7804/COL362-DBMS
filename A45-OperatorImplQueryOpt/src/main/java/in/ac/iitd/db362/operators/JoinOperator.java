package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

import java.util.*;

/**
 * The join operator performs a Hash Join.
 * TODO: Implement the open(), next(), and close() methods.
 *
 * Do not change the constructor and member variables or getters
 * Do not remove logging! otherwise your test cases will fail!
 */
public class JoinOperator extends OperatorBase implements Operator {
    private Operator leftChild;
    private Operator rightChild;
    private JoinPredicate predicate;

    private Map<Object, List<Tuple>> hashTable;
    private Tuple currentRightTuple;
    private List<Tuple> currentMatchingLeftTuples;
    private int currentLeftIndex;
    private int leftColumnIndex;
    private int rightColumnIndex;

    public JoinOperator(Operator leftChild, Operator rightChild, JoinPredicate predicate) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.predicate = predicate;
    }

    @Override
    public void open() {
        // Do not remove logging--
        logger.trace("Open()");
        // ----------------------

        //TODO: Implement me!
        leftChild.open();
        // Complete the building phase
        hashTable = new HashMap<>();
        Tuple leftTuple = leftChild.next();
        // initialise the leftColumnIndex and solve for the first tuple
        if (leftTuple != null) {
            leftColumnIndex = leftTuple.getSchema().indexOf(((EqualityJoinPredicate) predicate).getLeftColumn());
            if (leftColumnIndex == -1) {
                // column not found in the left relation
                throw new IllegalArgumentException("Column not found: " + ((EqualityJoinPredicate) predicate).getLeftColumn());
            }
            Object key_leftValue = leftTuple.getValues().get(leftColumnIndex);
            Object key = getHashKey(key_leftValue);
            if(key != null) {
                hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(leftTuple);
            }
        }
        // Solve for all the consecutive tuples
        while ((leftTuple = leftChild.next()) != null) {
            Object key_leftValue = leftTuple.getValues().get(leftColumnIndex);
            Object key = getHashKey(key_leftValue);
            if(key != null) {
                hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(leftTuple);
            }
        }

        // Initialise the probing phase
        rightChild.open();
        currentRightTuple = null;
        currentMatchingLeftTuples = null;
        currentLeftIndex = 0;
        rightColumnIndex = -1;

    }

    @Override
    public Tuple next() {
        // Do not remove logging--
        logger.trace("Next()");
        // ----------------------

        //TODO: Implement me!
        while (true) {
            if (currentMatchingLeftTuples != null && currentLeftIndex < currentMatchingLeftTuples.size()) {
                Tuple leftTuple = currentMatchingLeftTuples.get(currentLeftIndex);
                currentLeftIndex++;
                List<String> joinedSchema = new ArrayList<>(leftTuple.getSchema());
                joinedSchema.addAll(currentRightTuple.getSchema());
                List<Object> joinedValues = new ArrayList<>(leftTuple.getValues());
                joinedValues.addAll(currentRightTuple.getValues());
                return new Tuple(joinedValues, joinedSchema);
            } else {
                currentRightTuple = rightChild.next();
                if (currentRightTuple == null) {
                    return null;
                }
                // Set rightColumnIndex from the first right tuple
                if (rightColumnIndex < 0) {
                    rightColumnIndex = currentRightTuple.getSchema().indexOf(((EqualityJoinPredicate) predicate).getRightColumn());
                    if (rightColumnIndex == -1) {
                        throw new IllegalArgumentException("Column not found: " + ((EqualityJoinPredicate) predicate).getRightColumn());
                    }
                }
                Object rightValue = currentRightTuple.getValues().get(rightColumnIndex);
                Object rightKey = getHashKey(rightValue);
                currentMatchingLeftTuples = hashTable.get(rightKey);
                if (currentMatchingLeftTuples == null) {
                    currentMatchingLeftTuples = Collections.emptyList();
                }
                currentLeftIndex = 0;
            }
        }
    }

    @Override
    public void close() {
        // Do not remove logging ---
        logger.trace("Close()");
        // ------------------------

        //TODO: Implement me!
        leftChild.close();
        rightChild.close();
        hashTable = null;
        currentRightTuple = null;
        currentMatchingLeftTuples = null;
    }
    // Not using the evaluate() function in EqualityJoinPredicate - no use for that here since we're not comparing tuple-to-tuple
    // This implementation assumes that the left-handed relation is smaller (i.e. has lower number of records)

    private Object getHashKey(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Integer) {
            return ((Integer) value);
        } else if (value instanceof Double) {
            return ((Double) value);
        } else if (value instanceof String) {
            return value;
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass().getSimpleName());
        }
    }

    // Do not remove these methods!
    public Operator getLeftChild() {
        return leftChild;
    }

    public Operator getRightChild() {
        return rightChild;
    }

    public JoinPredicate getPredicate() {
        return predicate;
    }
}
