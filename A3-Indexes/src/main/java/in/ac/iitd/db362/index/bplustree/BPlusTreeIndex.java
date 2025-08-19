package in.ac.iitd.db362.index.bplustree;

import in.ac.iitd.db362.index.Index;
import in.ac.iitd.db362.parser.Operator;
import in.ac.iitd.db362.parser.QueryNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.time.LocalDate;

/**
 * Starter code for BPlusTree Implementation
 * @param <T> The type of the key.
 */
// Testing incomplete + doubts! (plus implement delete and binary search)
public class BPlusTreeIndex<T> implements Index<T> {

    protected static final Logger logger = LogManager.getLogger();

    // Note: Do not rename this variable; the test cases will set this when testing. You can however initialize it with a
    // different value for testing your code.
    // Set to 3 for testing (originally 10)
    public static int ORDER = 10;

    // The attribute being indexed
    private String attribute;

    // Our Values are all integers (rowIds)
    private Node<T, Integer> root;
    private final int order; // Maximum children per node

    /** Constructor to initialize the B+ Tree with a given order */
    public BPlusTreeIndex(String attribute) {
        System.out.println("Initializing BPlusTree with order " + ORDER);
        this.attribute = attribute;
        this.order = ORDER;
        this.root = new Node<>();
        this.root.isLeaf = true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Integer> evaluate(QueryNode node) {
        logger.info("Evaluating predicate using B+ Tree index on attribute " + attribute + " for operator " + node.operator);
        System.out.println("Evaluating query: " + node.operator + " " + node.value);
        List<Integer> resultSet = new ArrayList<>();
        T parsedKey = (T) parseKey(node.value);

        if (node.operator == Operator.EQUALS) {
            return search(parsedKey);
        } else if (node.operator == Operator.LT) {
            T minKey = getMinKey();
            if(minKey == null) {
                return resultSet;
            }
            return rangeQuery(minKey, true, parsedKey, false);
        } else if (node.operator == Operator.GT) {
            T maxKey = getMaxKey();
            if(maxKey == null) {
                return resultSet;
            }
            return rangeQuery(parsedKey, false, maxKey, true);
        } else if (node.operator == Operator.RANGE) {
            T parsedEndKey = (T) parseKey(node.secondValue);
            return rangeQuery(parsedKey, false, parsedEndKey, false);
        } else {
            return resultSet;
        }
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

    private T getMinKey() {
        Node<T, Integer> current = root;
        if(root.keys == null) {
            return null;
        }
        while (!current.isLeaf) {
            current = current.children.get(0);
        }
        return current.keys.get(0); // First key in the leftmost leaf node
    }

    private T getMaxKey() {
        Node<T, Integer> current = root;
        if(root.keys == null) {
            return null;
        }
        while (!current.isLeaf) {
            current = current.children.get(current.children.size() - 1);
        }
        return current.keys.get(current.keys.size() - 1); // Last key in the rightmost leaf node
    }

    @Override
    public void insert(T key, int rowId) {
        //TODO: Implement me!
        System.out.println("Inserting key: " + key + ", RowID: " + rowId);

        // Inserting the first record i.e. root does not have any keys
        if (root.keys == null) {
            root.keys = new ArrayList<>();
            root.values = new ArrayList<>();
            root.keys.add(key);
            root.values.add(rowId);
            System.out.println("Inserted in root: " + root.keys);
            return;
        }

        // Find the leaf node where the key belongs
        // Keep track of the path from root to leaf (i.e. the parents which are in danger of splitting)
        List<Node<T, Integer>> path = new ArrayList<>();

        Node<T, Integer> current_node = root;
        path.add(current_node);

        while(!current_node.isLeaf) {
            int offset = getOffset_non_leaf(key, current_node.keys);
            System.out.println("Traversing internal node: " + current_node.keys + ", taking child at offset " + offset);
            current_node = current_node.getChild(offset);
            path.add(current_node);
        }
        // Easy case - no overflow
        insertIntoLeaf(current_node, key, rowId);
        // Overflow
        if (isOverflow(current_node)) {
            handleLeafOverflow(current_node, path);
        }

    }

    /* Insert a key in some leaf node */
    private void insertIntoLeaf(Node<T, Integer> leaf_node, T key, int rowId) {
        System.out.println("Inserting into leaf: " + key);
        if(leaf_node.keys == null) {
            leaf_node.keys = new ArrayList<>();
            leaf_node.values = new ArrayList<>();
        }
        int index = 0;
        // Find correct insertion index using linear search for now (convert to binary search later)
        while(index < leaf_node.keys.size() && compareKey(leaf_node.keys.get(index), key) < 0) {
            index++;
        }
        leaf_node.keys.add(index, key);
        leaf_node.values.add(index, rowId);
        System.out.println("Leaf after insert: " + leaf_node.keys);
    }

    /* Check for overflow */
    private boolean isOverflow(Node<T, Integer> node) {
        return node.keys.size() >= this.order;
    }

    /* Handle Leaf Overflow */
    private void handleLeafOverflow(Node<T, Integer> leaf, List<Node<T, Integer>> path) {
        // Creating a new leaf node and splitting the original node
        Node<T, Integer> new_leaf_node = new Node<>();
        new_leaf_node.isLeaf = true;
        int ori_n = leaf.keys.size();

        // Split keys and values
        int mid = ori_n / 2; // Actually ori_n = order
        new_leaf_node.keys = new ArrayList<>(leaf.keys.subList(mid, ori_n));
        new_leaf_node.values = new ArrayList<>(leaf.values.subList(mid, ori_n));
        leaf.keys.subList(mid, ori_n).clear();
        leaf.values.subList(mid, ori_n).clear();

        // Get the next pointers correctly - it is disappointing that there is a getNext() but no setNext()
        new_leaf_node.next = leaf.getNext();
        leaf.next = new_leaf_node;

        T keyToProp = new_leaf_node.keys.get(0);
        path.remove(path.size()-1);

        if(!path.isEmpty()) {
            Node<T, Integer> parent = path.get(path.size()-1);
            // Handle inserting into a parent node
            insertIntoParent(parent, keyToProp, leaf, new_leaf_node, path);
        }
        else {
            Node<T, Integer> newRoot = new Node<>();
            newRoot.isLeaf = false;
            newRoot.keys = new ArrayList<>();
            newRoot.children = new ArrayList<>();

            newRoot.keys.add(keyToProp);
            newRoot.children.add(leaf);
            newRoot.children.add(new_leaf_node);

            root = newRoot;
        }
    }

    /* Insert a key into it's parent - occurs due to overflow */
    private void insertIntoParent(Node<T, Integer> parent, T key, Node<T, Integer> leftChild, Node<T, Integer> rightChild, List<Node<T, Integer>> path) {
        int index = 0;
        // Find correct insertion index using linear search for now (convert to binary search later)
        while(index < parent.keys.size() && compareKey(parent.keys.get(index), key) < 0) {
            index++;
        }
        parent.keys.add(index, key);
        parent.children.add(index + 1, rightChild);

        if (isOverflow(parent)) {
            handleParentOverflow(parent, path);
        }
    }

    /* Handle parent Overflow */
    private void handleParentOverflow(Node<T, Integer> node, List<Node<T, Integer>> path) {
        Node<T, Integer> newNode = new Node<>();
        newNode.isLeaf = false;
        int n = node.keys.size();

        // Confusion between the following two ways: (going with (1) for now)
        // (1) mid is done on the basis of the applet i.e. split with higher keys in right but take the first key of the right node to it's  parent
        // (2) another way to do this would be to separate the key, and segregate the remaining to left/right with right >= left
        int mid = (n-1) / 2;
        T keyToProp = node.keys.get(mid);

        newNode.keys = new ArrayList<>(node.keys.subList(mid + 1, n));
        newNode.children = new ArrayList<>(node.children.subList(mid+1, n));
        node.keys.subList(mid, n).clear();
        node.children.subList(mid+1, n).clear();

        path.remove(path.size()-1);

        if(!path.isEmpty()) {
            Node<T, Integer> parent = path.get(path.size()-1);
            insertIntoParent(parent, keyToProp, node, newNode, path);
        }
        else {
            // Create a new root if we have no parent
            Node<T, Integer> newRoot = new Node<>();
            newRoot.isLeaf = false;
            newRoot.keys = new ArrayList<>();
            newRoot.children = new ArrayList<>();

            newRoot.keys.add(keyToProp);
            newRoot.children.add(node);
            newRoot.children.add(newNode);

            root = newRoot;
        }
    }

    @Override
    public boolean delete(T key) {
        //TODO: Bonus
        return false;
    }

    @Override
    public List<Integer> search(T key) {
        //TODO: Implement me!
        //Note: When searching for a key, use Node's getChild() and getNext() methods. Some test cases may fail otherwise!
        //Assuming no duplicates for my implementation, will always return list with at most a single value
        System.out.println("Searching for key: " + key);
        List<Integer> resultSet = new ArrayList<>();
        if(root.keys == null) {
            System.out.println("Search failed, tree is empty.");
            return resultSet; //Edge case when there are no keys in the B+ Tree
        }
        Node <T, Integer> current_node = root;
        while(!current_node.isLeaf) {
            List<T> nl_keys = current_node.keys;
            int nl_offset = getOffset_non_leaf(key, nl_keys);
            System.out.println("Traversing internal node: " + current_node.keys + ", taking child at offset " + nl_offset);
            current_node = current_node.getChild(nl_offset);
        }
        List<T> l_keys = current_node.keys;
        List<Integer> l_values = current_node.values;
        int l_offset = getOffset_leaf(key, l_keys);
        if(l_offset == -1) {
            System.out.println("Key not found in leaf node.");
            return resultSet;
        }
        else {
            int value = l_values.get(l_offset);
            resultSet.add(value);
            System.out.println("Key found at leaf: " + current_node.keys + " -> RowID: " + resultSet);
            return resultSet;
        }
    }

    /**
     * Function that evaluates a range query and returns a list of rowIds.
     * e.g., 50 < x <=75, then function can be called as rangeQuery(50, false, 75, true)
     * @param startKey
     * @param startInclusive
     * @param endKey
     * @param endInclusive
     * @return all rowIds that satisfy the range predicate
     */
    List<Integer> rangeQuery(T startKey, boolean startInclusive, T endKey, boolean endInclusive) {
        //TODO: Implement me!
        //Note: When searching, use Node's getChild() and getNext() methods. Some test cases may fail otherwise!
        List<Integer> resultSet = new ArrayList<>();
        if(root.keys == null) {
            return resultSet; //Edge case when there are no keys in the B+ Tree
        }
        Node <T, Integer> current_node = root;
        while(!current_node.isLeaf) {
            List<T> nl_keys = current_node.keys;
            int offset = getOffset_nl_range(startKey, nl_keys);
            current_node = current_node.getChild(offset);
        }

        boolean done = false;
        while(!done && current_node != null) {
            List<T> keys = current_node.keys;
            int n = keys.size();
            List<Integer> values = current_node.values;

            // If the leaf node doesn't have any key >= startKey, ignore this leaf
            if(compareKey(keys.get(n-1), startKey) < 0) {
                current_node = current_node.getNext();
                continue;
            }

            int k_index = 0; //k_index is the smallest key with key >= startKey
            // Find the first key that is >= startKey
            while (k_index < n && compareKey(keys.get(k_index), startKey) < 0) {
                k_index++;
            }
            // Handle Inclusivity, if exact match found
            if (k_index < n && compareKey(keys.get(k_index), startKey) == 0 && !startInclusive) {
                k_index++; // Skip if startInclusive is false
            }
            // Optimized insertion, linear scan only required in first and last leaf
            if (compareKey(keys.get(n - 1), endKey) < 0) {
                // If last key < endKey, add all remaining values
                resultSet.addAll(values.subList(k_index, n));
            } else {
                // Otherwise, collect values selectively
                while (k_index < n && compareKey(keys.get(k_index), endKey) <= 0) {
                    if (compareKey(keys.get(k_index), endKey) == 0 && !endInclusive) {
                        done = true;
                        break;
                    }
                    resultSet.add(values.get(k_index));
                    k_index++;
                }
            }
            // This condition is valid as we don't have duplicate search keys
            if (compareKey(keys.get(n - 1), endKey) >= 0) {
                done = true;
            } else {
                current_node = current_node.getNext();
            }
        }
        return resultSet;
    }

    /**
     * Traverse leaf nodes and collect all keys in sorted order
     * @return all Keys
     */
    public List<T> getAllKeys() {
        List<T> allKeys = new ArrayList<>();
        Node<T, Integer> current = root;

        // Traverse to the leftmost leaf
        while (!current.isLeaf) {
            current = current.children.get(0);
        }

        // Traverse through all leaf nodes and collect keys
        while (current != null) {
            allKeys.addAll(current.keys);
            current = current.getNext(); // Move to next leaf
        }

        return allKeys;
    }

    /**
     * Compute tree height by traversing from root to leaf
     * @return Height of the b+ tree
     */
    public int getHeight() {
        int height = -1; //
        Node<T, Integer> current = root;

        while (current != null) {
            height++;
            if (current.isLeaf) {
                break;
            }
            current = current.children.get(0); // Move to the first child
        }

        return height;
    }

    /**
     * Funtion that returns the order of the BPlusTree
     * Note: Do not remove this function!
     * @return
     */
    public int getOrder() {
        return order;
    }

    /*
    * Helper method to get the offset of the required key in leaf node
    * @return offset if key exists, else returns -1
     */
    private int getOffset_leaf(T key, List<T> keys) {
        // Implementing linear search for now, will convert to binary search if time permits
        int offset = 0;
        int n = keys.size();
        for(int i = 0; i < n; i++) {
            if(compareKey(key, keys.get(i)) == 0) {
                offset = i;
                return offset;
            }
            if(compareKey(key, keys.get(i)) < 0) {
                offset = -1;
                return offset;
            }
        }
        offset = -1;
        return offset;
    }

    /*
    * Helper method to get the offset of the required child in a non leaf node in SEARCH
    * @return offset of the child pointer which has key (keys in pi < ki <= keys in pi+1)
     */
    private int getOffset_non_leaf(T key, List<T> keys) {
        // Implementing linear search for now, will convert to binary search if time permits
        int offset = 0;
        int n = keys.size();
        for (int i = 0; i < n; i++) {
            if(compareKey(key, keys.get(i)) < 0) {
                offset = i;
                return offset;
            }
        }
        offset = n;
        return offset;
    }

    /*
     * Helper method to get the offset of the required child in a non leaf node in RANGE QUERY
     * @return offset of the child pointer which has key (keys in pi < ki <= keys in pi+1)
     */
    private int getOffset_nl_range(T key, List<T> keys) {
        // Implementing linear search for now, will convert to binary search if time permits
        int offset = 0;
        // Find the smallest i such that key ≤ keys[i]
        while (offset < keys.size() && compareKey(key, keys.get(offset)) > 0) {
            offset++;
        }
        if (offset == keys.size()) {
            return keys.size();
        } else if (compareKey(key, keys.get(offset)) == 0) {
            // If key == keys[i], return the next pointer
            return offset + 1;
        } else {
            return offset;
        }
    }


    /*
     * Helper method to compare keys - Ensures output is strictly -1, 0, or 1.
     * @return -1 if key1 < key2, 0 if equal, 1 if key1 > key2
     */
    private int compareKey(T key1, T key2) {
        // We have left the "numeric" type comparisons, but I think we need to handle them as well...
        if (key1 instanceof Integer && key2 instanceof Integer) {
            return Integer.signum(((Integer) key1).compareTo((Integer) key2));
        }
        else if (key1 instanceof Double && key2 instanceof Double) {
            return Integer.signum(((Double) key1).compareTo((Double) key2));
        }
        else if (key1 instanceof String && key2 instanceof String) {
            return Integer.signum(((String) key1).compareTo((String) key2));
        }
        else if (key1 instanceof LocalDate && key2 instanceof LocalDate) {
            return Integer.signum(((LocalDate) key1).compareTo((LocalDate) key2));
        }
        else {
            throw new IllegalArgumentException("Unsupported key types: " + key1.getClass() + " and " + key2.getClass());
        }
    }



    @Override
    public String prettyName() {
        return "B+Tree Index";
    }
}
