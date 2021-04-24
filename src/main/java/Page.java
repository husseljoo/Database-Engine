import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable 
{

	int max=this.pageEntries();
	Vector<Tuple> tuples=new Vector<Tuple>();
	Page nextPage;
	
	public Page(){}

	public void insert(Tuple l)
	{
		if(tuples.size()>=max)
			return;
		tuples.add(l);
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
	
	
	public static void main(String[] args) {
		Page p=new Page();
//		int n=p.pageEntries();
		System.out.println(p.max);
	}
	
	
	
	
	
	
	
	
	
	
	
}

