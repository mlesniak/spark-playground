package com.mlesniak.spark;

import com.mlesniak.runner.BaseRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

public class Main extends BaseSparkPlayground {

    public static void main(String[] args) throws Exception {
        BaseRunner.initRunner(Configuration.class, "spark-playground", args);
        new Main().run();
    }

    public Main() {
        super();
    }

    private void run() throws Exception {
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
