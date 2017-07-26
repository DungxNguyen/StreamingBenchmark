package kinesis.common;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class RecordTemplate {
	public static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final Random RANDOM = new Random();
	private String level;
	private String timestamp;
	private String cat;
	private String msg;
	private int id;
	private long time;

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public static RecordTemplate genRandomData(int size) {
		RecordTemplate record = new RecordTemplate();
		if (RANDOM.nextInt(3) != 0) {
			record.setLevel("INFO");
		} else {
			record.setLevel("WARN");
		}
		record.setTimestamp(SDF.format(new Date(System.currentTimeMillis())));
		return record;
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getCat() {
		return cat;
	}

	public void setCat(String cat) {
		this.cat = cat;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
}
