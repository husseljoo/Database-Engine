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
	
	
	

	public static void main(String[] args) {

		Tuple t=new Tuple();
		String s="1,Abo-Hedar,1,2,3,Food,Heliopolis";
		t.addRecord(s);


	
        String filename = "/home/husseljo/Desktop/DB2Project/src/main/resources/testTuple.ser"; //testTuple.class also works?
	          
        try
	        {   
        	//Saving of object in a file
	        FileOutputStream file = new FileOutputStream(filename);
	        ObjectOutputStream out = new ObjectOutputStream(file);
	          
	        // Method for serialization of object
	        out.writeObject(t);
	          
	        out.close();
	        file.close();
	          
	        System.out.println("Object has been serialized");

	        }
	          
	    catch(IOException ex)
	        {
	        ex.printStackTrace();
	        System.out.println("IOException is caught");
	        }
	
	
    	Tuple tup=null;
     // Deserialization
        try
        {   
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filename);
            ObjectInputStream in = new ObjectInputStream(file);
              
            // Method for deserialization of object
            try{tup= (Tuple)in.readObject();}catch(Exception e) {}
              
            in.close();
            file.close();
              
            System.out.println("Object has been deserialized ");
            System.out.println("Vector is= " + tup.record.toString());
 
        }
          
        catch(IOException ex)
        {
        	ex.printStackTrace();
            System.out.println("IOException is caught");
        }
	
	
	
	



	}

}






