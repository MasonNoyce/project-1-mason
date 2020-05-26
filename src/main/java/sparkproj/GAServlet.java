package sparkproj;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.IntegerType;

import static org.apache.spark.sql.functions.*;



@WebServlet("/GAServlet")
public class GAServlet extends HttpServlet {
	SparkSession spark;
	Dataset<Row> ds;
	

	@Override
	public void init() throws ServletException {
		//CreateSparkContext
		SparkConf conf = new SparkConf().setAppName("HurricaneNames").setMaster("local");
		this.spark = SparkSession.builder().appName("HurricaneNames").config(conf).getOrCreate();
		ds = this.spark.read().option("header","true").csv("hurricane.csv");
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		  // Set response content type



		resp.getWriter().println("Hello!");
		String col1 = req.getParameter("HCol1");
		System.out.println("Get Request = " + col1);
		String col2 = req.getParameter("HCol2");
		String num = req.getParameter("HNum");
		
		int rnum = 100;
		if(!num.equals(""))

			rnum = Integer.parseInt(num);

		
		
		System.out.println("Get Request2 = " + col2);

		resp.getWriter().println("Hurricane Type: " + col1);
		resp.getWriter().println("Hurricane Var: " + col2);

		//compare the averages of one column based on another
		Dataset<Row> types = ds.groupBy(col1).agg(avg(col2)).orderBy(avg(col2));
	

		String result = types.showString(rnum, 10, false);
		System.out.println("The Results ARE IN!");

		resp.getWriter().println(result);

	}


	@Override
	public void destroy()
	{
		spark.close();
	}

}
