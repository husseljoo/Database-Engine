import java.io.Serializable;
import java.util.Vector;

public class IndexInfo implements Serializable
{
	String indexName;
	Vector<String> indexedCol; //name of all columns
	Vector<Integer> colNum;    //their positions
	Object[][] minmax;
	
	public IndexInfo(String indexName, String[] col)
	{
		this.indexName=indexName;
		indexedCol = new Vector<String>();
		for(int i =0;i<col.length;i++)
		{
			indexedCol.add(col[i]);
		}
	}
	
}
