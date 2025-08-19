package in.ac.iitd.db362.index;

import in.ac.iitd.db362.parser.Operator;
import in.ac.iitd.db362.parser.QueryNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.ArrayList;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Starter code for a BitMap Index
 * Bitmap indexes are typically used for equality queries and rely on a BitSet.
 *
 * @param <T> The type of the key.
 */
public class BitmapIndex<T> implements Index<T> {

    protected static final Logger logger = LogManager.getLogger();

    private final Class<T> type;

    private String attribute;
    private int maxRowId;

    private Map<T, int[]> bitmaps;

    /**
     * Constructor
     *
     * @param type
     * @param attribute
     * @param maxRowId
     */
<<<<<<< Updated upstream
    public BitmapIndex(Class<T> type, String attribute, int maxRowId) {
        this.type = type;
=======
    public BitmapIndex(String attribute, int maxRowId) {
        System.out.println("Initializing BitmapIndex for attribute: " + attribute + " with maxRowId: " + maxRowId);
>>>>>>> Stashed changes
        this.attribute = attribute;
        this.maxRowId = maxRowId;
        bitmaps = new HashMap<>();
    }

    /**
     * Create a empty bitmap for a given key
     * @param key
     */
    private void createBitmapForKey(T key) {
        int arraySize = (maxRowId + 31) / 32;
        bitmaps.putIfAbsent(key, new int[arraySize]);
        System.out.println("Created new bitmap for key: " + key);
    }


    /**
     * This has been done for you.
     * @param key The attribute value.
     * @param rowId The row ID associated with the key.
     */
    public void insert(T key, int rowId) {
        System.out.println("Inserting into BitmapIndex - Key: " + key + " | RowID: " + rowId);
        createBitmapForKey(key);
        int index = rowId / 32;
        int bitPosition = rowId % 32;
        bitmaps.get(key)[index] |= (1 << bitPosition);
        System.out.println("Updated Bitmap for " + key + ": " + bitmaps.get(key)[index] + " (Index: " + index + ", Bit: " + bitPosition + ")");
    }


    @Override
    /**
     * This is only for completeness. Although one can delete a key, it will mess up rowIds
     * If a record is deleted, then an unset bit may lead to ambiguity (is false vs not exists)
     */
    public boolean delete(T key) {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Integer> evaluate(QueryNode node) {
        logger.info("Evaluating predicate using Bitmap index on attribute " + attribute + " for operator " + node.operator);
        // TODO: implement me
        if (node.operator == Operator.EQUALS) {
            Object parsedKey = parseKey(node.value);
            // unchecked at compile time, but will see that it holds in runtime (manual correctness)
            return search((T) parsedKey);
        }
        return new ArrayList<>();
    }

    // We don't know the type of key unless at runtime
    private Object parseKey(String key) {
        System.out.println("Parsing key: " + key);
        if (key.matches("-?\\d+")) {
            return Integer.parseInt(key);
        } else if (key.matches("-?\\d+\\.\\d+")) {
            return Double.parseDouble(key);
        } else if (key.matches("\\d{4}-\\d{2}-\\d{2}")) {
            try {
                return LocalDate.parse(key);
            } catch (Exception e) {
                return key; // If parsing fails, return as String
            }
        } else {
            return key; // Default to String if no match
        }
    }

    @Override
    public List<Integer> search(T key) {
    //TODO: Implement me!
        System.out.println("Searching BitmapIndex for key: " + key);
        List<Integer> result = new ArrayList<>();
        // retrieve bitmap for the given key
        int[] bitmap = bitmaps.get(key);
        if (bitmap == null) {
            System.out.println("Key " + key + " not found in BitmapIndex.");
            return result;
        }
        System.out.println("Bitmap state for key '" + key + "': " + java.util.Arrays.toString(bitmap));
        for(int i = 0; i < bitmap.length; i++) {
            int bits = bitmap[i];
            if(bits == 0) {
                continue;
            }
            for (int j = 0; j < 32; j++) {
                if ((bits & (1 << j)) != 0) {
                    // Calculate the rowId from the index and bit position.
                    int rowId = i * 32 + j;
                    // Ensure we don't return rowIds that are out of bounds.
                    if (rowId <= maxRowId) {
                        result.add(rowId);
                        System.out.println("Bit set found - RowID: " + rowId);
                    }
                }
            }
        }
        System.out.println("Search complete for key " + key + ". Found RowIDs: " + result);
        return result;
    }

    @Override
    public String prettyName() {
        return "BitMap Index";
    }
}