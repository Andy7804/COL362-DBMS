package in.ac.iitd.db362.index.hashindex;

import in.ac.iitd.db362.index.Index;
import in.ac.iitd.db362.parser.Operator;
import in.ac.iitd.db362.parser.QueryNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;


/**
 * Starter code for Extendible Hashing
 * @param <T> The type of the key.
 */
public class ExtendibleHashing<T> implements Index<T> {

    protected static final Logger logger = LogManager.getLogger();

    private final Class<T> type;

    private String attribute; // attribute that we are indexing

   // Note: Do not rename the variable! You can initialize it to a different value for testing your code.
    public static int INITIAL_GLOBAL_DEPTH = 10;


    // Note: Do not rename the variable! You can initialize it to a different value for testing your code.
    public static int BUCKET_SIZE = 4;

    private int globalDepth;

    // directory is the bucket address table backed by an array of bucket pointers
    // the array offset (can be computed using the provided hashing scheme) allows accessing the bucket
    private Bucket<T>[] directory;


    /** Constructor */
    @SuppressWarnings("unchecked")
    public ExtendibleHashing(Class<T> type, String attribute) {
        this.type = type;
        this.globalDepth = INITIAL_GLOBAL_DEPTH;
        int directorySize = 1 << globalDepth;
        this.directory = new Bucket[directorySize];
        for (int i = 0; i < directorySize; i++) {
            directory[i] = new Bucket<>(globalDepth);
        }
        this.attribute = attribute;
    }

    // This is a basic hash implementation, it does not assume duplicates and is based on the extended hashing mechanism discussed in class
    // Also, overflow buckets are not used in this case - simplified implementation
    @SuppressWarnings("unchecked")
    @Override
    public List<Integer> evaluate(QueryNode node) {
        logger.info("Evaluating predicate using Hash index on attribute " + attribute + " for operator " + node.operator);
        // TODO: Implement me!
        if (node.operator == Operator.EQUALS) {
            Object parsedKey = parseKey(node.value);
            // unchecked at compile time, but will see that it holds in runtime (manual correctness)
            return search((T) parsedKey);
        }
        return null;
    }

    // We don't know the type of key unless at runtime
    private Object parseKey(String key) {
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
    public void insert(T key, int rowId) {
        // TODO: Implement insertion logic with bucket splitting and/or doubling the address table
        int d_index = getDirectoryIndexHelper(key, globalDepth);
        Bucket<T> d_bucket = directory[d_index];

        while(!insertIntoBucket(d_bucket, key, rowId)) {
            if(d_bucket.localDepth == globalDepth) {
                doubleDirectory();
            }
            handleBucketSplit(d_index);

            // recompute and try to insert in the next loop
            d_index = getDirectoryIndexHelper(key, globalDepth);
            d_bucket = directory[d_index];
        }

    }

    /* helper to insert into a bucket */
    private boolean insertIntoBucket(Bucket<T> bucket, T key, int rowId) {
        int n = bucket.size;
        if(n < BUCKET_SIZE) {
            bucket.keys[n] = key;
            bucket.values[n] = rowId;
            bucket.size++;
            return true;
        }
        // Unable to insert into bucket - probably need to split
        return false;
    }

    /* doubles the directory and re-assigns all the bucket pointers */
    @SuppressWarnings("unchecked")
    private void doubleDirectory() {
        int oldSize = directory.length;
        int newSize = oldSize * 2;
        Bucket<T>[] newDirectory = (Bucket<T>[]) new Bucket[newSize];
        for (int i = 0; i < oldSize; i++) {
            newDirectory[i] = directory[i];
            newDirectory[i + oldSize] = directory[i];
        }
        directory = newDirectory;
        globalDepth++;
    }

    private void handleBucketSplit(int d_index) {
        Bucket<T> bucket_to_split = directory[d_index];
        int oldLD = bucket_to_split.localDepth;
        bucket_to_split.localDepth++;
        int newLD = bucket_to_split.localDepth;

        Bucket<T> newBucket = new Bucket<>(newLD);

        // collect keys and values from the bucket - to be used later
        List<T> o_keys = new ArrayList<>();
        List<Integer> o_values = new ArrayList<>();
        for (int i = 0; i < bucket_to_split.size; i++) {
            o_keys.add(bucket_to_split.keys[i]);
            o_values.add(bucket_to_split.values[i]);
        }

        bucket_to_split.size = 0;

        // re-assign directory points
        int dir_n = directory.length;
        for(int i = 0; i < dir_n; i++) {
            if(directory[i] == bucket_to_split) {
                if (((i >> (newLD - 1)) & 1) == 1) {
                    directory[i] = newBucket;
                }
            }
        }
        // re-insert all keys from original bucket into appropriate bucket
        for(int i = 0; i < o_keys.size(); i++) {
            T o_key = o_keys.get(i);
            int o_rowId = o_values.get(i);
            int newIndex = getDirectoryIndexHelper(o_key, globalDepth);
            Bucket<T> n_bucket = directory[newIndex];
            insertIntoBucket(n_bucket, o_key, o_rowId);
        }
    }


    @Override
    public boolean delete(T key) {
        // TODO: (Bonus) Implement deletion logic with bucket merging and/or shrinking the address table
        // Implemented basic delete function without merging/shrinking
        int index = getDirectoryIndexHelper(key, globalDepth);
        Bucket<T> bucket = directory[index];

        // Find the key in the bucket
        int pos = -1;
        for (int i = 0; i < bucket.size; i++) {
            if (compareKey(bucket.keys[i], key) == 0) {
                pos = i;
                break;
            }
        }
        if (pos == -1) {
            // Key not found i.e. deletion unsuccessful
            return false;
        }

        // Remove the key by shifting subsequent keys left
        for (int i = pos; i < bucket.size - 1; i++) {
            bucket.keys[i] = bucket.keys[i + 1];
            bucket.values[i] = bucket.values[i + 1];
        }
        bucket.size--;
        return true;
    }


    @Override
    public List<Integer> search(T key) {
        // TODO: Implement search logic
        int d_index = getDirectoryIndexHelper(key, globalDepth);
        Bucket<T> d_bucket = directory[d_index];
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < d_bucket.size; i++) {
            if (d_bucket.keys[i].equals(key)) {
                result.add(d_bucket.values[i]);
            }
        }
        return result;
    }

    /* helper function to get DirectoryIndex regardless of key type */
    private int getDirectoryIndexHelper(T key, int globalDepth) {
        if (key instanceof Integer) {
            return HashingScheme.getDirectoryIndex((Integer) key, globalDepth);
        }
        else if (key instanceof Double) {
            return HashingScheme.getDirectoryIndex((Double) key, globalDepth);
        }
        else if (key instanceof String) {
            return HashingScheme.getDirectoryIndex((String) key, globalDepth);
        }
        else if (key instanceof LocalDate) {
            return HashingScheme.getDirectoryIndex((LocalDate) key, globalDepth);
        }
        else {
            // fallback : convert to string and get the answer by default
            return HashingScheme.getDirectoryIndex(key.toString(), globalDepth);
        }
    }

    /* comparison function (same as the one in BPlusTreeIndex.java) */
    private int compareKey(T key1, T key2) {
        if (key1 instanceof Integer && key2 instanceof Integer) {
            return Integer.signum(((Integer) key1).compareTo((Integer) key2));
        } else if (key1 instanceof Double && key2 instanceof Double) {
            return Integer.signum(((Double) key1).compareTo((Double) key2));
        } else if (key1 instanceof String && key2 instanceof String) {
            return Integer.signum(((String) key1).compareTo((String) key2));
        } else if (key1 instanceof LocalDate && key2 instanceof LocalDate) {
            return Integer.signum(((LocalDate) key1).compareTo((LocalDate) key2));
        } else {
            throw new IllegalArgumentException("Unsupported key types: "
                    + key1.getClass() + " and " + key2.getClass());
        }
    }

    /**
     * Note: Do not remove this function!
     * @return
     */
    public int getGlobalDepth() {
        return globalDepth;
    }

    /**
     * Note: Do not remove this function!
     * @param bucketId
     * @return
     */
    public int getLocalDepth(int bucketId) {
        return directory[bucketId].localDepth;
    }

    /**
     * Note: Do not remove this function!
     * @return
     */
    public int getBucketCount() {
        return directory.length;
    }


    /**
     * Note: Do not remove this function!
     * @return
     */
    public Bucket<T>[] getBuckets() {
        return directory;
    }

    public void printTable() {
        // TODO: You don't have to, but its good to print for small scale debugging
        System.out.println("Global Depth: " + globalDepth);
        for (int i = 0; i < directory.length; i++) {
            System.out.print("Dir[" + i + "] -> Bucket(localDepth=" + directory[i].localDepth + ", keys: ");
            Bucket<T> bucket = directory[i];
            for (int j = 0; j < bucket.size; j++) {
                System.out.print(bucket.keys[j] + " ");
            }
            System.out.println(")");
        }
    }

    @Override
    public String prettyName() {
        return "Hash Index";
    }

}