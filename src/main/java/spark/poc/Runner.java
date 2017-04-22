package spark.poc;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Runner {

	public static void main(String[] args) {
		Runner runner = new Runner();
		runner.run();
	}

	private void run() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.set("spark.logConf", "true");
		sparkConf.set("spark.driver.host", "localhost");
		SparkSession sparkSession = SparkSession.builder().master("local[*]").config(sparkConf).getOrCreate();
		//jdbc:oracle:thin:hr/hr@//localhost:1521/POC_DB
		String jdbcUrl = "jdbc:mysql://localhost:3306/POC_DB";
		String username = "root";
		String password = "";
		String dbtable = "test";
		Dataset<Row> dataset = loadDBTable(sparkSession, jdbcUrl, username, password, dbtable);
		dataset.show(1, false);
	}

	public static Dataset<Row> loadDBTable(SparkSession sparkSession, String jdbcUrl, String username, String password,
			String dbtable) {
		String format = "jdbc";
		//oracle.jdbc.driver.OracleDriver
		String driver = "com.mysql.jdbc.Driver";
		Dataset<Row> loadedDataset = null;
		loadedDataset = sparkSession.read().format(format).option("url", jdbcUrl).option("dbtable", dbtable)
				.option("user", username).option("password", password).option("driver", driver).load();
		return loadedDataset;
	}

}
