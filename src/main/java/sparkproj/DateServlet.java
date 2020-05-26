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
import static org.apache.spark.sql.functions.*;



@WebServlet("/DateServlet")
public class DateServlet extends HttpServlet {
	SparkSession spark;
	Dataset<Row> ds;
	

	@Override
	public void init() throws ServletException {
		//CreateSparkContext
		SparkConf conf = new SparkConf().setAppName("HurricaneDates").setMaster("local");
		this.spark = SparkSession.builder().appName("HurricaneDates").config(conf).getOrCreate();
		ds = this.spark.read().option("header","true").csv("hurricane.csv");
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		  // Set response content type



		resp.getWriter().println("Hello!");
		String var = req.getParameter("HVar");
		String num = req.getParameter("HNum");
		
		int rnum = 1000;
		if(!num.equals(""))
			rnum = Integer.parseInt(num);

		System.out.println("Get Request = " + var);
		resp.getWriter().println("Hurricane var: " + var);
		//group wind from lowest to highest based on browser request
		Dataset<Row> vars = ds.filter(col("Maximum Wind").gt(var));
		String result = vars.showString(rnum, 10, false);
		System.out.println("The Results ARE IN!");
		
		resp.getWriter().println(result);


	}


	@Override
	public void destroy()
	{
		spark.close();
	}

}
