package com.mlesniak.spark;

import com.mlesniak.runner.BaseRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

public class Main extends BaseRunner {
    private final Configuration conf;

    public static void main(String[] args) throws Exception {
        BaseRunner.initRunner(Configuration.class, "spark-playground", args);
        new Main().run();
    }

    public Main() {
        conf = Configuration.get();
    }

    private void run() throws Exception {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[8]")
                .setAppName("spark-playground");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaSQLContext sql = new org.apache.spark.sql.api.java.JavaSQLContext(sc);

        JavaSchemaRDD file = sql.parquetFile(conf.getDirectory());
        file.cache();
        file.registerTempTable("data");

        long count = file.count();
        info("count {}", count);

        JavaSchemaRDD keys = sql.sql("select key, value from data where key = 123 order by value");
        info("count with key = 123: {}", keys.count());

        JavaRDD<String> json = keys.toJSON();
        json.saveAsTextFile(conf.getDirectory() + "/../out");
    }
}
