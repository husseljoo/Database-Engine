import java.io.Serializable;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable 
{

	int N=2; //move to tableInfo
	Vector<Tuple> tuples=new Vector<Tuple>();
	
	public Page()
	{
		this.N=2;
	}
	

	public boolean isFull() {
		return N==tuples.size();
	}
	
	public void insert(Tuple l)
	{
		if(tuples.size()>=N)
			return;
		tuples.add(l);
	}
	
	public void addTuple(Tuple t) {
		tuples.add(t);
	}
	
	
//READ DBapp.config file and extract number of entries per page to set the instance variable
//max of class Page to this value
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
		int entries=Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		
		return entries;
		}
	
	
	
}

