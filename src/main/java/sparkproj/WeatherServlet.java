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



@WebServlet("/WeatherServlet")
public class WeatherServlet extends HttpServlet {
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
		String type = req.getParameter("HType");
		System.out.println("Get Request = " + type);
		String var = req.getParameter("HVar");
		String num = req.getParameter("HNum");
		
		int rnum = 1000;
		if(!num.equals(""))
			rnum = Integer.parseInt(num);
		
		
		System.out.println("Get Request2 = " + var);
		resp.getWriter().println("Hurricane Type: " + type);
		resp.getWriter().println("Hurricane Var: " + var);

		Dataset<Row> types = ds.filter(col(type).contains(var.toUpperCase()));
		String result = types.showString(rnum, 10, false);
		System.out.println("The Results ARE IN!");
//		System.out.println(result);
		resp.getWriter().println(result);

	}


	@Override
	public void destroy()
	{
		spark.close();
	}

}
