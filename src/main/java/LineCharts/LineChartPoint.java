package LineCharts;

public class LineChartPoint {
	double Y;
	String X;
	String grapheName;
	
	public LineChartPoint(double Y, String X, String grapheName){
		this.Y = Y;
		this.X = X;
		this.grapheName = grapheName;
	}
	
	public void setY(double y){
		this.Y = y;
	}
	
	public void setX(String x){
		this.X = x;
	}
	
	public void setGname(String g){
		this.grapheName = g;
	}
}
