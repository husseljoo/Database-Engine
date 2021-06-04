import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class DDVector implements Serializable
{
	Vector<Object> array;
	int dim;
	int bucketNumber = 0;
	public DDVector(int dimensions)
	{
		int size = 10;
		array = new Vector<Object>();
		//array.add(null);
		if(dimensions==1)
			{
			dim = 1;
			size =1;
			}
		else
		{
			dim = dimensions;
			for(int i=0;i<size;i++)
				array.add(i,new DDVector(dimensions-1));
		}
	}
	public String toString()
	{
		if(this.dim ==1)
		{
			String s ="";
			for(int i =0;i<array.size();i++)
			{		if(array.get(i)==null)
					s+="null,";
				else
					s+=array.get(i).toString();
			}
			return "{ "+s+" },";
		}
		else
		{
			String s ="";
			for(int i =0;i<array.size();i++)
			{	if(array.get(i)==null)
					s+="null,";
				else
					s+=array.get(i).toString();
			}
			return "{ "+s+" },";
		}
	}
	public void insertAtDimensions(int[] locations, Object[] value)
	{
		DDVector level = this;
		//check if size of array = dimen
		int dimension = this.dim;
		for(int i=0;i<locations.length;i++)
			level = (DDVector) level.array.get(locations[i]);
		
		level.array.add(value);
		bucketNumber++;
		//level.array.insertElementAt(value, 0);
	}
	
	
	
	
	public Vector getFromDimensions(int[] locations)
	{
		DDVector level = this;
		//check if size of array = dimen
		int dimension = this.dim;
		for(int i=0;i<locations.length;i++)
			level = (DDVector) level.array.get(locations[i]);
		
		return level.array;
	}
	
	public static void main(String[] args) 
	{
		DDVector x = new DDVector(2);
		int [] y= {5};
		Object[] b1 = {"bucket1",false};
		Object[] b2 = {"bucket2",true};
		x.insertAtDimensions(y, b1);
		x.insertAtDimensions(y, b2);
		System.out.println(x.toString());
		Vector c = x.getFromDimensions(y);
	}

}
