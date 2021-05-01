import java.io.Serializable;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable 
{

	int N=this.pageEntries(); //move to tableInfo
	
	Vector<Tuple> tuples=new Vector<Tuple>();
	
	public Page()
	{
		this.N=3;
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
	
	
	public static void main(String[] args) {
		Page p=new Page();
		
		Tuple t0=new Tuple();
		String s0="1,Abo-Hedar,1,2,3,Food,Heliopolis";
		t0.addRecord(s0);
		
		Tuple t1=new Tuple();
		String s1="1,Gad,1,2,3,Food,Nasr City";
		t1.addRecord(s1);

		Tuple t2=new Tuple();
		String s2="1,Arabiata,1,2,3,Food,Dokki";
		t2.addRecord(s2);

		Tuple t3=new Tuple();
		String s3="1,Waffilicious,1,2,3,Food,Sheraton";
		t3.addRecord(s3);
	
		p.addTuple(t0);
		p.addTuple(t1);
		p.addTuple(t2);
		p.addTuple(t3);
	
		System.out.println(p.tuples.toString());
		Tuple tup=p.tuples.get(0);
		System.out.println(tup.record.toString());
		
	}
	

	
	
	
	
	
	
	
	
	
}

