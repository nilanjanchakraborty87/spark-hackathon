import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Optional;

public class Reports {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("HackerNews");
		sparkConf.setMaster("local");
		JavaSparkContext spark = new JavaSparkContext(sparkConf);

		groupByYear(spark);
	}

	private static void groupByYear(JavaSparkContext spark) {
		spark.textFile("/home/tomasz/tmp/comments.csv")
				.map(Comment::fromString)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.groupBy(comment -> comment.getCreatedAt().getYear())
				.map(pair -> new Tuple2<>(pair._1(), Iterables.size(pair._2()))).
		saveAsTextFile("/home/tomasz/tmp/spark/by_year.txt");
	}

	private static void wordCount(JavaSparkContext spark) {
		final JavaPairRDD<String, Integer> result = spark.textFile("/home/tomasz/tmp/comments.csv")
				.flatMap(line -> Arrays.asList(line.split(" ")))
				.mapToPair(s -> new Tuple2<>(s, 1))
				.reduceByKey((a, b) -> a + b); result
				.saveAsTextFile("/home/tomasz/tmp/result.csv");
	}

}
