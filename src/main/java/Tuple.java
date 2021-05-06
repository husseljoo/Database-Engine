import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

public class Tuple implements Serializable
{
	Vector<Object> record=new Vector<Object>();
	
	public Tuple() {}

	public void addRecord(String s) {
		this.record.add(s);
	}
	
	
}






