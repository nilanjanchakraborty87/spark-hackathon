import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.Map;

class Comment implements Serializable {

	private static final ObjectMapper jsonParser = new ObjectMapper();

	private final String author;
	private final String commentText;
	private final ZonedDateTime createdAt;
	private final int points;
	private final Long storyId;
	private final Long parentId;

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