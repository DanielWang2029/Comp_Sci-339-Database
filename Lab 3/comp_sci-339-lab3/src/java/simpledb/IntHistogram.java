package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int w_b;
    private int ntups;
    private int[] histogram;

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
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.w_b = (int) Math.ceil((double) (max-min + 1) / buckets);
        this.ntups = 0;
        this.histogram = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int targetbucket = (v - this.min) / this.w_b;
        if (v >= min && v <= max) {
            this.histogram[targetbucket]++;
            this.ntups++;
        }
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
        double counter = 0.0;
        int targetbucket = (v - this.min) / this.w_b;
    	// some code goes here
        switch(op) {
            case LIKE:
            case EQUALS:
                if (v < this.min || v > this.max) {return 0.0;}
                return ((double) this.histogram[targetbucket]) / this.w_b / this.ntups;
            case NOT_EQUALS:
                if (v < this.min || v > this.max) {return 1.0;}
                return 1.0 - ((double) this.histogram[targetbucket]) / this.w_b / this.ntups;
            case GREATER_THAN:
                if (v < this.min) {return 1.0;} else if (v >= this.max) {return 0.0;}
                counter += this.histogram[targetbucket] * ((double) (this.min + targetbucket * this.w_b + this.w_b - 1 - v) / this.w_b);
                for (int i = targetbucket + 1; i < this.buckets; i++) {
                    counter += this.histogram[i];
                }
                return counter / this.ntups;
            case LESS_THAN:
                if (v <= this.min) {return 0.0;} else if (v > this.max) {return 1.0;}
                counter += this.histogram[targetbucket] * ((double) (v - this.min - targetbucket * this.w_b) / this.w_b);
                for (int i = targetbucket - 1; i >= 0; i--) {
                    counter += this.histogram[i];
                }
                return counter / this.ntups;
            case GREATER_THAN_OR_EQ:
                if (v <= this.min) {return 1.0;} else if (v > this.max) {return 0.0;}
                counter += this.histogram[targetbucket] * ((double) (this.min + targetbucket * this.w_b + this.w_b - v) / this.w_b);
                for (int i = targetbucket + 1; i < this.buckets; i++) {
                    counter += this.histogram[i];
                }
                return counter / this.ntups;
            case LESS_THAN_OR_EQ:
                if (v < this.min) {return 0.0;} else if (v >= this.max) {return 1.0;}
                counter += this.histogram[targetbucket] * ((double) (v - this.min - targetbucket * this.w_b + 1) / this.w_b);
                for (int i = targetbucket - 1; i >= 0; i--) {
                    counter += this.histogram[i];
                }
                return counter / this.ntups;
            default:
                //??????
                return -1.0;
        }
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
