import java.util.Vector;

public class Index {
	
	Vector grid;
	Vector<String> columnNames;
	String str_TableName;
	int dimensions;
	
	public Index(String str_TableName, int dimensions, Vector<String> columnNames) {
		this.dimensions=dimensions;
		this.str_TableName=str_TableName;
		this.columnNames=columnNames;
		this.grid=new Vector<Object>(10);
		
		if(dimensions!=1) {
			for(int i=0;i<dimensions-1;i++) {
				
				for(int j=0;j<9;j++) {
					Vector<Object> v=new Vector<Object>(10);
					grid.add(i, v);
				}
				
			}
		}
	
	
	
	}





}
