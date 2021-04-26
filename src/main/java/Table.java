import java.io.Serializable;
import java.util.Vector;

public class Table 
{
//	Vector<Serializable> pages=new Vector<Serializable>();
	Vector<String[]> pages=new Vector<String[]>();
	
	public Table(){}
	
	public void addPage(String[] tuple) 
	{
		this.pages.add(tuple);
	}
			
	
	
}
