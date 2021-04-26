import java.util.ArrayList;

public class TableInfo 
{
	public ArrayList<String> colOrder;
	public ArrayList<String> maxInPages;
	public String clusteringKey;
	
	public TableInfo()
	{
		colOrder = new ArrayList<String>();
		maxInPages = new ArrayList<String>();
		clusteringKey= "";
	}

	public void setColOrder(ArrayList<String> colOrder) {
		this.colOrder = colOrder;
	}

	public void setMaxInPages(ArrayList<String> maxInPages) {
		this.maxInPages = maxInPages;
	}

	public void setClusteringKey(String clusteringKey) {
		this.clusteringKey = clusteringKey;
	}

	
	
	
	
	
	
	
	
	
	
}
