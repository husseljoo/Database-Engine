import java.io.Serializable;
import java.util.Vector;

public class TableInfo implements Serializable
{
	public Vector<String> colOrder;
	public Vector<Object[]> pages;
	public String clusteringKey;
	
	public TableInfo()
	{
		colOrder = new Vector<String>();
		pages = new Vector<Object[]>();
		clusteringKey= "";
	}

	public void setColOrder(Vector<String> colOrder) {
		this.colOrder = colOrder;
	}
}
