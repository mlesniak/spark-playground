package com.mlesniak.spark;

import com.mlesniak.runner.BaseRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;

/**
 * Base class for different spark playgrounds.
 *
 * @author Michael Lesniak (mail@mlesniak.com)
 */
public class BaseSparkPlayground extends BaseRunner {
    protected final Configuration conf;
    protected final JavaSQLContext sql;
    protected final JavaSparkContext sc;
    protected final SparkConf sparkConf;

    public BaseSparkPlayground() {
        sparkConf = new SparkConf()
                .setMaster("local[8]")
                .setAppName("spark-playground");
        sc = new JavaSparkContext(sparkConf);
        sql = new JavaSQLContext(sc);
        conf = Configuration.get();
    }
}
