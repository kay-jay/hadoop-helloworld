package kayjay.helloworld;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FsOperations {

	private static final Logger logger = LoggerFactory
			.getLogger(FsOperations.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("core-site.xml");
		logger.info(conf.get("fs.default.name"));
		FileSystem fs = FileSystem.get(conf);
		Path dir = new Path("user/hadoop/cdr");
		try {
			boolean created = fs.mkdirs(dir);
			logger.info("mkdirs returned {}", created);
			boolean exists = fs.exists(dir);
			logger.info("exists returned {}", exists);
			fs.copyFromLocalFile(
					new Path(FsOperations.class.getResource("/testFile.txt")
							.toURI()), new Path("user/hadoop/cdr/testFile.txt"));
		} finally {
			fs.delete(dir, true);
			fs.close();
		}
	}

}
