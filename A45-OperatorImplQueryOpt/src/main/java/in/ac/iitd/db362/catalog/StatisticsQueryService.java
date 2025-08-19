package in.ac.iitd.db362.catalog;

/**
 *
 * MODIFY/UPDATE -- DO WHAT EVER YOU WANT WITH THIS CLASS!
 *
 * This class provides some methods that the optimizer can use to query the catalog.
 * The starter code is only for your reference. **Feel free** to modify this by
 * implementing the methods * and/or by adding your own methods that you think your
 * optimizer can use when optimizing plans.
 */
public class StatisticsQueryService {
    private final Catalog catalog;

    public StatisticsQueryService(Catalog catalog) {
        this.catalog = catalog;
    }

    public double getEqualitySelectivity(String tableName, String columnName, Object value) {
        TableStatistics tableStats = catalog.getTableStatistics(tableName);
        if (tableStats == null) {
            return 0.5; // Default if no statistics available
        }

        ColumnStatistics<?> colStats = tableStats.getColumnStatistics(columnName);
        if (colStats == null) {
            return 0.5; // Default if no column statistics available
        }

        // Basic selectivity assumption: 1/cardinality
        int cardinality = colStats.getCardinality();
        if (cardinality <= 0) {
            return 0.1; // Default for empty columns
        }

        return 1.0 / cardinality;
    }

    public double getEqualitySelectivityUsingHistogram(String tableName, String columnName, Object value) {
        TableStatistics tableStats = catalog.getTableStatistics(tableName);
        if (tableStats == null) {
            return getEqualitySelectivity(tableName, columnName, value);
        }
        ColumnStatistics<?> colStats = tableStats.getColumnStatistics(columnName);
        if (colStats == null) {
            return getEqualitySelectivity(tableName, columnName, value);
        }
        int[] histogram = colStats.getHistogram();
        if (histogram == null || histogram.length == 0) {
            return getEqualitySelectivity(tableName, columnName, value);
        }
        // Determine the bucket for this value
        Object min = colStats.getMin();
        Object max = colStats.getMax();
        int bucketIndex = getBucketIndex(value, min, max, histogram.length);

        if (bucketIndex < 0 || bucketIndex >= histogram.length) {
            return 0.0; // Value outside the range
        }

        // Estimate based on bucket frequency and bucket size
        int bucketFrequency = histogram[bucketIndex];
        int totalValues = colStats.getNumValues();

        // Assume uniform distribution within the bucket
        double bucketSelectivity = (double) bucketFrequency / totalValues;
        double valueSelectivity = bucketSelectivity / getBucketSize(bucketIndex, histogram.length);

        return Math.min(valueSelectivity, 1.0);
    }


    public double getRangeSelectivity(String tableName, String columnName, Object lowerBound, Object upperBound) {
        TableStatistics tableStats = catalog.getTableStatistics(tableName);
        if (tableStats == null) {
            return 0.5; // Default
        }

        ColumnStatistics<?> colStats = tableStats.getColumnStatistics(columnName);
        if (colStats == null) {
            return 0.5; // Default
        }

        Object min = colStats.getMin();
        Object max = colStats.getMax();

        if (min == null || max == null) {
            return 0.33; // Common default
        }

        // Normalize bounds within the column's range
        double normalizedLower = (lowerBound != null) ? normalize(lowerBound, min, max) : 0.0;
        double normalizedUpper = (upperBound != null) ? normalize(upperBound, min, max) : 1.0;

        // Limit to valid range [0, 1]
        normalizedLower = Math.max(0.0, Math.min(1.0, normalizedLower));
        normalizedUpper = Math.max(0.0, Math.min(1.0, normalizedUpper));

        // Calculate fraction of range
        return Math.max(0.0, normalizedUpper - normalizedLower);
    }


    public double getRangeSelectivityUsingHistogram(String tableName, String columnName, Object lowerBound, Object upperBound) {
        TableStatistics tableStats = catalog.getTableStatistics(tableName);
        if (tableStats == null) {
            return getRangeSelectivity(tableName, columnName, lowerBound, upperBound);
        }

        ColumnStatistics<?> colStats = tableStats.getColumnStatistics(columnName);
        if (colStats == null) {
            return getRangeSelectivity(tableName, columnName, lowerBound, upperBound);
        }

        int[] histogram = colStats.getHistogram();
        if (histogram == null || histogram.length == 0) {
            return getRangeSelectivity(tableName, columnName, lowerBound, upperBound);
        }

        Object min = colStats.getMin();
        Object max = colStats.getMax();

        // Find the bucket indices
        int lowerBucketIndex = (lowerBound != null) ?
                getBucketIndex(lowerBound, min, max, histogram.length) : 0;
        int upperBucketIndex = (upperBound != null) ?
                getBucketIndex(upperBound, min, max, histogram.length) : histogram.length - 1;

        // Constrain to valid range
        lowerBucketIndex = Math.max(0, lowerBucketIndex);
        upperBucketIndex = Math.min(histogram.length - 1, upperBucketIndex);

        // If completely outside range
        if (lowerBucketIndex > upperBucketIndex) {
            return 0.0;
        }

        // Sum the frequencies in the covered buckets
        int coveredCount = 0;
        int totalCount = colStats.getNumValues();

        for (int i = lowerBucketIndex; i <= upperBucketIndex; i++) {
            coveredCount += histogram[i];
        }

        return (double) coveredCount / totalCount;
    }


    public Object getMin(String tableName, String columnName) {
        TableStatistics tableStats = catalog.getTableStatistics(tableName);
        if (tableStats == null) {
            return null;
        }

        ColumnStatistics<?> colStats = tableStats.getColumnStatistics(columnName);
        if (colStats == null) {
            return null;
        }

        return colStats.getMin();
    }


    public Object getMax(String tableName, String columnName) {
        TableStatistics tableStats = catalog.getTableStatistics(tableName);
        if (tableStats == null) {
            return null;
        }

        ColumnStatistics<?> colStats = tableStats.getColumnStatistics(columnName);
        if (colStats == null) {
            return null;
        }

        return colStats.getMax();
    }

    /**
     * Helper method to determine which bucket a value belongs to
     */
    private int getBucketIndex(Object value, Object min, Object max, int numBuckets) {
        double normalizedValue = normalize(value, min, max);
        if (normalizedValue < 0 || normalizedValue > 1) {
            return -1; // Outside the range
        }

        return (int) (normalizedValue * numBuckets);
    }

    /**
     * Helper method to normalize a value within a range to [0,1]
     */
    private double normalize(Object value, Object min, Object max) {
        if (value instanceof Number && min instanceof Number && max instanceof Number) {
            double val = ((Number) value).doubleValue();
            double minVal = ((Number) min).doubleValue();
            double maxVal = ((Number) max).doubleValue();

            if (maxVal == minVal) {
                return (val == minVal) ? 0.5 : (val < minVal ? 0.0 : 1.0);
            }

            return (val - minVal) / (maxVal - minVal);
        } else if (value instanceof String && min instanceof String && max instanceof String) {
            String val = (String) value;
            String minVal = (String) min;
            String maxVal = (String) max;

            if (maxVal.equals(minVal)) {
                return val.equals(minVal) ? 0.5 : (val.compareTo(minVal) < 0 ? 0.0 : 1.0);
            }

            // Simple string range estimation
            double valLength = val.length();
            double minLength = minVal.length();
            double maxLength = maxVal.length();
            double rangeSize = maxLength - minLength;

            if (rangeSize == 0) {
                // Use lexicographic comparison as an approximation
                return (val.compareTo(minVal) - minVal.compareTo(minVal)) /
                        (double) (maxVal.compareTo(minVal) - minVal.compareTo(minVal));
            }

            return (valLength - minLength) / rangeSize;
        }

        return 0.5; // Default if types don't match
    }

    /**
     * Helper to get the relative size of a bucket
     */
    private double getBucketSize(int bucketIndex, int numBuckets) {
        return 1.0 / numBuckets; // Assuming equal width buckets
    }
}
