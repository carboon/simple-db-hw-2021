package simpledb.optimizer;

import simpledb.execution.Predicate;

import static simpledb.execution.Predicate.Op.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private double [] bucketArray;
    private double gap;
    private int count;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.min = min;
        this.max = max;
        this.buckets = buckets;
        this.bucketArray = new double[buckets];
        this.gap = (max + 1 - min)* 1.0/buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if(v >max || v < min)return;
    	bucketArray[(int)((v - min)*1.0/gap)]++;
        count++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
//        int index = (v - min)/gap;
        double res = 0.0;
        switch (op) {
            case GREATER_THAN: {
                if(v > max) return 0.0;
                if(v < min) return 1.0;
                int index = (int) (((v - min)*1.0)/gap);
                double sum = 0;
                for (int i = index+1; i < buckets; i++) {
                    sum += bucketArray[i];
                }
                double estimate_0 = bucketArray[index] * ((double)(index+1)*gap - v)/gap ;
                res = (estimate_0 + sum);
                break;
            }
            case EQUALS: {
                if (v > max || v < min) return 0.0;
                int index = (int)((v - min)*1.0/gap);
                double estimate = bucketArray[index] / gap ;
                res = estimate;
                break;
            }
            case LESS_THAN: {
                if(v > max) return 1.0;
                if(v < min) return 0.0;
                int index = (int) (((v - min)*1.0)/gap);
                double sum = 0;
                for (int i = 0; i < index; i++) {
                    sum += bucketArray[i];
                }
                double estimate_0 = bucketArray[index] * ((v - index*gap )/gap);
                res = (estimate_0 + sum);
                break;
            }
            case LESS_THAN_OR_EQ: {
                return estimateSelectivity(LESS_THAN,v) + estimateSelectivity(EQUALS,v);
            }
            case GREATER_THAN_OR_EQ: {
                return estimateSelectivity(GREATER_THAN,v) + estimateSelectivity(EQUALS,v);
            }

            case NOT_EQUALS: {
                return  1.0 - estimateSelectivity(EQUALS,v);
            }
        }
        return res/count;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
