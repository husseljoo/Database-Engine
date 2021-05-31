import java.io.Serializable;
import java.util.Properties;
import java.util.Vector;

public class Bucket implements Serializable {
	
	Vector<Object[]> tupleReferences;
	int N=this.pageEntries(); //move to tableInfo
	
	public Bucket()
	{
		this.tupleReferences=new Vector<Object[]>();
		//this.N=this.pageEntries();
		this.N=2;
	}
	
	public void insert(Object[] l)
	{
		if(this.isFull()) 
		{
			return;
		}
		else
			tupleReferences.add(l);
	}
	
	public boolean isFull() {
		return N==tupleReferences.size();
	}
	public int pageEntries() {
		
		String config="DBApp.config";
		Properties prop=new Properties();
		
		prop = new java.util.Properties();
		try {
		   prop.load(this.getClass().getClassLoader().
		   getResourceAsStream(config));
		 }
		catch(Exception e){
		     e.printStackTrace();
		 }
		int entries=Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
		
		return entries;
		}
	
}
