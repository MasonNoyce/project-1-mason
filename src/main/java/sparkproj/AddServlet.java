package sparkproj;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;



@WebServlet("/AddServlet")
public class AddServlet extends HttpServlet {
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

		  //Combine Datasets
		  resp.getWriter().println("Submitted!");
		  String ID = req.getParameter("HID");
		  String name = req.getParameter("HName");
		  String date = req.getParameter("HDate");
		  String time = req.getParameter("HTime");
		  String event = req.getParameter("HEvent");
		  String status = req.getParameter("HStatus");
		  String longi = req.getParameter("HLong");
		  String lat = req.getParameter("HLat");

		  //open hurricane file
		  File file = new File("hurricane.csv");
		  //print new hurricane to end
		  PrintWriter pw = new PrintWriter(new FileWriter(file, true));
		  pw.println();
		  //print to browser
		  pw.println(ID.toUpperCase() + ", " +  name.toUpperCase() + ", " + date.toUpperCase()
		   + ", " + time.toUpperCase() + ", " + event.toUpperCase() + ", " + status.toUpperCase() + ", " + longi.toUpperCase() + ", " + 
		   lat.toUpperCase()
		  + ",    ,     ,    ,    ,    ,    ,    , -999, -999, -999, -999,-999, -999, -999, -999, -999, -999, -999, -999, -999" );

		pw.close();



	}


	@Override
	public void destroy()
	{
		spark.close();
	}

}
