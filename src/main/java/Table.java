import java.io.Serializable;
import java.util.Vector;

public class Table 
{
	Vector<Serializable> pages=new Vector<Serializable>();
	
	public Table(){}
	
	public void addPage(Serializable tuple) {
		this.pages.add(tuple);
		}
			
	
	
}
