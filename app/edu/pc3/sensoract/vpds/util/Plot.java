package edu.pc3.sensoract.vpds.util;

import java.awt.BasicStroke;
import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;

import edu.pc3.sensoract.vpds.tasklet.LuaToJavaFunctionMapper;

public class Plot {

	// create a plot, save it and send the file name
	public static String createPlot(Map<Long, Double> data, String title,
			String unit) {

		if (data == null || data.isEmpty()) {
			return null;
		}

		String filename = null;
		
		try {
		//System.out.println(JsonUtil.json.toJson(data));
		
		TimeSeries ts = new TimeSeries(title);

		long epoch;
		Date time;
		double val;

		for (Long key : data.keySet()) {
			
			//System.out.println("adding..." + key);
			epoch = key.longValue();
			time = new Date(epoch);
			
			//val = Double.parseDouble(data.get(key));
			val = data.get(key);
			ts.addOrUpdate(new Millisecond(time), val);
			
		}

		TimeSeriesCollection tsCol = new TimeSeriesCollection();
		tsCol.addSeries(ts);

		JFreeChart chart = createJFreeChart(tsCol, title, unit);
			
			String path = play.Play.applicationPath.getCanonicalPath() + "/plots/";
			filename = title + new java.util.Date().getTime();			
			filename = path + filename + ".png";

			LuaToJavaFunctionMapper.LOG.info("Creating plot " + filename);			
			//String decodedPath = URLDecoder.decode(path, "UTF-8");			
			ChartUtilities.saveChartAsPNG(new File(filename).getCanonicalFile(), chart, 800, 400);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LuaToJavaFunctionMapper.LOG.error("Plot " + e.getMessage());
			e.printStackTrace();
		}		
		return filename;
	}

	private static JFreeChart createJFreeChart(XYDataset dataset, String title,
			String unit) {

		// System.out.println(dataset);
		JFreeChart chart = ChartFactory.createTimeSeriesChart(title, // title
				"Time", // x-axis label
				unit, // y-axis label
				dataset, // data
				false, // create legend?
				true, // generate tooltips?
				false // generate URLs?
				);
		
		
		chart.setBackgroundPaint(Color.white);

		XYPlot xyPlot = (XYPlot) chart.getPlot();
		xyPlot.setBackgroundPaint(Color.white);
		xyPlot.setDomainGridlinePaint(Color.black);
		xyPlot.setRangeGridlinePaint(Color.black);
		// plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));
		xyPlot.setDomainCrosshairVisible(true);
		xyPlot.setRangeCrosshairVisible(true);
		
        XYItemRenderer xyir = xyPlot.getRenderer();
        xyir.setSeriesStroke(0, new BasicStroke(3.1f)); //series line style
        
        xyir.setSeriesPaint(0, new Color(47,117,250));

        		
		XYItemRenderer r = xyPlot.getRenderer();
		if (r instanceof XYLineAndShapeRenderer) {
			XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
			// renderer.setSeriesStroke(0, new BasicStroke(1.1f));
			// plot.setRenderer(renderer);
		}

		DateAxis axis = (DateAxis) xyPlot.getDomainAxis();
		axis.setDateFormatOverride(new SimpleDateFormat("d-M-yy HH:mm:ss"));

		return chart;
	}

}
