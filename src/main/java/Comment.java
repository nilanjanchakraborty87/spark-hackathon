import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.*;

class Comment implements Serializable {

	private static final ObjectMapper jsonParser = new ObjectMapper();

	private final String author;
	private final String commentText;
	private final ZonedDateTime createdAt;
	private final int points;
	private final Long storyId;
	private final Long parentId;

	public static Optional<Comment> fromCsv(String input) {
		try {
			final String[] columns = split(input);
			if (columns.length != 6) {
				throw new IllegalArgumentException("Wrong no of columns: " + columns.length + " in " + Arrays.toString(columns));
			}
			final String author = columns[0];
			final String commentText = columns[1];
			final ZonedDateTime date = parseIsoDate(columns[2]);
			final int points = Integer.parseInt(columns[3]);
			final Long storyId = parseLong(columns[4]);
			final Long parentId = parseLong(columns[5]);
			return Optional.of(new Comment(author, commentText, date, points, storyId, parentId));
		} catch (Exception e) {
			return Optional.empty();
		}
	}

	static Comment fromJson(String json) {
		try {
			final Map<String, Object> map = jsonParser.readValue(json, Map.class);
			return new Comment(
					map.get("author").toString(),
					map.get("comment_text").toString(),
					parseIsoDate(map.get("created_at").toString()),
					((Number)map.get("points")).intValue(),
					toLong(map.get("story_id")),
					toLong(map.get("parent_id"))
			);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static Long toLong(Object val) {
		if (val != null) {
			return ((Number) val).longValue();
		} else {
			return null;
		}
	}

	private static Long parseLong(String column) {
		if (column != null && !column.trim().isEmpty()) {
			return Long.parseLong(column.trim());
		} else {
			return null;
		}
	}

	private static String[] split(String input) {
		final ArrayList<String> columns = new ArrayList<>(Arrays.asList(input.split(",")));
		return mergeQuoted(columns);
	}

	/**
	 * Merge parts in case comma inside quotes
	 */
	private static String[] mergeQuoted(ArrayList<String> columns) {
		final String[] mergedColumns = new String[6];
		int curColumn = 0;
		for (int i = 0; i < columns.size(); i++, curColumn++) {
			if (isStartWithoutEnd(columns, i)) {
				final int closingIndex = findClosingIndex(columns, i);
				mergedColumns[curColumn] = stripQuotes(mergeBetween(columns, i, closingIndex));
				i = closingIndex;
			} else {
				mergedColumns[curColumn] = stripQuotes(columns.get(i));
			}
		}
		return mergedColumns;
	}

	private static boolean isStartWithoutEnd(ArrayList<String> columns, int i) {
		final String s = columns.get(i);
		return s.startsWith("\"") && (!s.endsWith("\"") || s.length() == 1);
	}

	private static String stripQuotes(String s) {
		if (s.startsWith("\"") && s.endsWith("\"") && s.length() > 2) {
			return s.substring(1, s.length() - 1);
		} else {
			return s;
		}
	}

	private static String mergeBetween(List<String> columns, int startingIndex, int closingIndex) {
		final List<String> quotedColumns = columns.subList(startingIndex, closingIndex + 1);
		StringBuilder result = new StringBuilder();
		for (String quotedColumn : quotedColumns) {
			result.append(quotedColumn).append(",");
		}
		return result.toString();
	}

	private static int findClosingIndex(ArrayList<String> columns, int startIdx) {
		for (int i = startIdx + 1; i < columns.size(); i++) {
			if (columns.get(i).endsWith("\"")) {
				return i;
			}
		}
		throw new IllegalArgumentException("Unterminated quote in " + columns);
	}

	private static ZonedDateTime parseIsoDate(String column) {
		return ZonedDateTime.parse(column);
	}

	private Comment(String author, String commentText, ZonedDateTime createdAt, int points, Long storyId, Long parentId) {
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

	public ZonedDateTime getCreatedAt() {
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

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("Comment{");
		sb.append("author='").append(author).append('\'');
		sb.append(", commentText='").append(commentText).append('\'');
		sb.append(", createdAt=").append(createdAt);
		sb.append(", points=").append(points);
		sb.append(", storyId=").append(storyId);
		sb.append(", parentId=").append(parentId);
		sb.append('}');
		return sb.toString();
	}

	public static void main(String[] args) throws IOException {
		final long start = System.currentTimeMillis();
		final long count = Files
				.lines(Paths.get("/home/tomasz/tmp/spark/hn_hits.json"))
				.skip(1)
				.map(Comment::fromJson)
				.count();

		System.out.println(count);
		System.out.println(System.currentTimeMillis() - start + "ms");

	}
}