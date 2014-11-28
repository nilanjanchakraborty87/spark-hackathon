import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.DayOfWeek;
import java.util.List;

public class Reports {

	public static String input;

	public static void main(String[] args) {
		input = args[0];
		SparkConf sparkConf = new SparkConf().setAppName("HackerNews");
//		sparkConf.setMaster("local[*]");
		sparkConf.setMaster("local");
		JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final long startTime = System.currentTimeMillis();

		byHour(spark).forEach(System.out::println);

		System.out.println(System.currentTimeMillis() - startTime + "ms");
	}

	private static List<Tuple2<Integer, Integer>> byHour(JavaSparkContext spark) {
		return loadAndParse(spark)
				.groupBy(comment -> comment.getCreatedAt().getHour())
				.map(pair -> new Tuple2<>(pair._1(), Iterables.size(pair._2())))
				.sortBy(Tuple2::_1, true, 1)
				.collect();
	}

	private static List<Tuple2<String, Integer>> keywordTrend(JavaSparkContext spark, String keyword) {
		return loadAndParse(spark)
				.filter(c -> c.getCommentText().toLowerCase().contains(keyword))
				.groupBy(Reports::monthKey)
				.map(pair -> new Tuple2<>(pair._1(), Iterables.size(pair._2())))
				.sortBy(Tuple2::_1, true, 1)
				.collect();
	}

	private static String monthKey(Comment comment) {
		return comment.getCreatedAt().getYear() + "-" + pad(comment.getCreatedAt().getMonthValue());
	}

	private static String pad(int value) {
		return (value < 10)? ("0" + value) : ("" + value);
	}

	private static List<Tuple2<DayOfWeek, Integer>> groupByDayOfWeek(JavaSparkContext spark) {
		return loadAndParse(spark)
				.groupBy(comment -> comment.getCreatedAt().getDayOfWeek())
				.map(pair -> new Tuple2<>(pair._1(), Iterables.size(pair._2())))
				.collect();
	}

	private static List<Tuple2<Integer, Integer>> groupByYear(JavaSparkContext spark) {
		return loadAndParse(spark)
				.groupBy(comment -> comment.getCreatedAt().getYear())
				.map(pair -> new Tuple2<>(pair._1(), Iterables.size(pair._2())))
				.collect();
	}

	private static JavaRDD<Comment> loadAndParse(JavaSparkContext spark) {
		return spark.textFile(input)
				.map(Comment::fromJson);
	}

}
