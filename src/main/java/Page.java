import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable 
{
	Vector<Tuple> tuples;
	int max;
	public Page(int n)
	{
		tuples = new Vector<Tuple>();
		max = n;
	}
	public void insert(Tuple l)
	{
		if(tuples.size()>=max)
			return;
		tuples.add(l);
	}
}
