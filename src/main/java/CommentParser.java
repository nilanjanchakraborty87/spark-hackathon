import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;

class Comment {
	private final String author;
	private final String commentText;
	private final Instant createdAt;
	private final int points;
	private final long storyId;
	private final long parentId;

	public static Comment fromString(String input) {
		final String[] columns = input.split(";");
		if (columns.length != 6) {
			throw new IllegalArgumentException(Arrays.toString(columns));
		}
		new Comment(columns[0], columns[1], parseIsoDate(columns[2]), Integer.parseInt(columns[3]), Long.parseLong(columns[4]), Long.parseLong(columns[5]));

	}

	private static Instant parseIsoDate(String column) {
		final TemporalAccessor parsed = DateTimeFormatter.ISO_INSTANT.parse(column);
		return Instant.from(parsed);
	}

	private Comment(String author, String commentText, Instant createdAt, int points, long storyId, long parentId) {
		this.author = author;
		this.commentText = commentText;
		this.createdAt = createdAt;
		this.points = points;
		this.storyId = storyId;
		this.parentId = parentId;
	}

	public String getAuthor() {
		return author;
	}

	public String getCommentText() {
		return commentText;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

	public int getPoints() {
		return points;
	}

	public long getStoryId() {
		return storyId;
	}

	public long getParentId() {
		return parentId;
	}

	public static void main(String[] args) {
		final String s = "\"VMG\",\"Because you don&#x27;t have to rely on a political apparatus to spend the money wisely.<p>By the way, nobody has to wait for billionaires anywhere, if you want to help out in education, get up and do it.\",\"2014-05-30T08:19:34Z\",1,7820350,7820656";
		new Comment()
	}
}