import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

public class TableInfo implements Serializable
{
	public Vector<String> colOrder;
	public Vector<Object[]> pages; //i.e: [[page0,"3"],[page1,"5"],[page2,"7"]]
	public String clusteringKey;
	public int clusterKeyIndex;
	public String clusterKeyType;
	int nonOverflowPageNum;
	int numOfPages;
	Vector<IndexInfo> indices;
	
	public TableInfo()
	{
		colOrder = new Vector<String>();
		pages = new Vector<Object[]>();
		indices = new Vector<IndexInfo>();
		clusteringKey= "";
		clusterKeyType = "";
		nonOverflowPageNum=0;
		numOfPages = 0;
	}

	public void setColOrder(Vector<String> colOrder) {
		this.colOrder = colOrder;
	}
	
}
