import java.io.Serializable;
import java.util.Vector;

public class TableInfo implements Serializable
{
	public Vector<String> colOrder;
	public Vector<Object[]> pages; //i.e: [[page0,"3"],[page1,"5"],[page2,"7"]]
	public String clusteringKey;
	public int clusterKeyIndex;
	
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
