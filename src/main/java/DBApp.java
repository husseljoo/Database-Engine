import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp implements DBAppInterface{
	FileWriter csvWriter;
	
	public void init()
	{
		String currLine ="Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max\n";
		try 
		{
			csvWriter = new FileWriter("/home/husseljo/Desktop/DB2Project/src/main/resources/metadata.csv");
			csvWriter.append(currLine);
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		
	}


    public void createTable(String str_TableName,
    		String str_ClusteringKeyColumn,
    		Hashtable<String,String> htbl_ColNameType,
    		Hashtable<String,String> htbl_ColNameMin,
    		Hashtable<String,String> htbl_ColNameMax )
    		throws DBAppException{
 
    	Enumeration<String> enumeration = htbl_ColNameType.keys();
    	String currLine = "";
        while(enumeration.hasMoreElements()) 
        {
        	String columnName= enumeration.nextElement();
        	String columnType=htbl_ColNameType.get(columnName);
        	String clusterKey=Boolean.toString(columnName.equals(str_ClusteringKeyColumn)); 
        	String columnMin=htbl_ColNameMin.get(columnName);
        	String columnMax=htbl_ColNameMax.get(columnName);
        	
        	currLine+= str_TableName+","+columnName+","+columnType+","+clusterKey+","+"False,"+columnMin+","+columnMax+"\n";
        	try 
        	{
				csvWriter.append(currLine);
			} 
        	catch (IOException e) 
        	{
				e.printStackTrace();
			}
        	currLine = "";
        }
        try 
        {
			csvWriter.flush();
		} catch (IOException e) 
        {
			e.printStackTrace();
		}
        /*********************************************************************************/
        //Once we create a Table, we create a folder/directory with its name inside the tables folder
        //it will contain all the pages of the table as well as a table_name.csv that contains info about
        // its page files (.class or .ser) i.e key range of page,bit indicating whether its full or not etc.
        try {

            Path path =Paths.get("/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName);
            Files.createDirectories(path);    
            
          } catch (Exception e) {
        	  e.printStackTrace();
            System.err.println("Failed to create directory!" + e.getMessage());

          }
        
        
        
        
        
        
        
    }


    public void insertIntoTable(String str_TableName,
    		Hashtable<String,Object> htbl_ColNameValue)
    		throws DBAppException{
    
    		boolean b=checkValidity(str_TableName,htbl_ColNameValue);
    		if(b) {
    			
    			//data is VALID: continue normal execution 
    			//stringify it and insert record
    		}
    	}    	
    
    
    //checks validity of insertIntoTable arguments
    //returns true if data is valid and throws DBAppException otherwise
    
    
    public static boolean checkValidity(String str_TableName,Hashtable<String,Object> htbl_ColNameValue) throws DBAppException{
    	
    	Path path =Paths.get("/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName);
    	if (!Files.exists(path)) {
    		throw new DBAppException("Specified Table does not exist at all!");
    		}
    	
    	BufferedReader objReader = null;
		String strCurrentLine;
		try {
			objReader = new BufferedReader(new FileReader("metadata.csv"));
			strCurrentLine = objReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
			}

		String[] arrOfStr = new String[0];
		int positionOfTable = 0;
		boolean found = false;
		ArrayList<String> fieldNames = new ArrayList();
		ArrayList<String> dataTypes = new ArrayList();
		ArrayList<String> min = new ArrayList();
		ArrayList<String> max = new ArrayList();
		Tuple tuple = new Tuple();
		try {
			for (int i = 0; (strCurrentLine = objReader.readLine()) != null; i++) {
				arrOfStr = strCurrentLine.split(",");
				if (arrOfStr[0].equals(str_TableName)) {
					positionOfTable = i;
					found = true;
				} else
					found=false;
				if (found) {
					fieldNames.add(arrOfStr[1]);  //Retrieve data info
					dataTypes.add(arrOfStr[2]);
					min.add(arrOfStr[5]);
					max.add(arrOfStr[6]);
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Enumeration<String> enumeration = htbl_ColNameValue.keys();
		String rawData;
		boolean b=false;
		
		while (enumeration.hasMoreElements()) {
			String columnName = enumeration.nextElement();
			for (int i = 0; i < fieldNames.size(); i++) {
				if (columnName.equals(fieldNames.get(i))) 
					rawData = (String) htbl_ColNameValue.get(columnName);
				else {
					continue;}
				if (dataTypes.get(i).equals("java.lang.Integer")) {
						int data;
						try {
							data = Integer.parseInt(rawData);
						} catch (Exception e) {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " of type integer");
						}
						if (data > Integer.parseInt(min.get(i)) && data < Integer.parseInt(max.get(i)))
							tuple.record.add(i, rawData);
						else {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " between "
									+ min.get(i) + " and " + max.get(i));
						}
					} else if (dataTypes.get(i).equals("java.lang.Double")) {
						double data;
						try {
							data = Double.parseDouble(rawData);
						} catch (Exception e) {
							System.out.println("Wrong input, please enter a value for " + columnName + " of type double");
							throw new DBAppException();
						}
						if (data > Double.parseDouble(min.get(i)) && data < Double.parseDouble(max.get(i)))
							tuple.record.add(i, rawData);
						else {
							System.out.println("Wrong input, please enter a value for " + columnName + " between "
									+ min.get(i) + " and " + max.get(i));
							throw new DBAppException();
						}
					} else if (dataTypes.get(i).equals("java.util.Date")) {
						Date data;
						SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
						try {
							data = (Date)formatter.parse(rawData);
						} catch (Exception e) {
							throw new DBAppException("Wrong input, please enter a value for " + columnName
									+ " of type date in the format " + '"' + "yyy-MM-dd" + '"');
						}
						try {
							if (data.after(formatter.parse(min.get(i))) && data.before(formatter.parse(max.get(i))))
								tuple.record.add(i, rawData);
							else {
								throw new DBAppException("Wrong input, please enter a value between " + columnName + " between "
										+ min.get(i) + " and " + max.get(i));
							}
						} catch (Exception e) {
							e.printStackTrace();
							throw new DBAppException();
						}
					} 
					//COMPARING ALL TYPES EXCEPT DATE
					else if (rawData.compareTo(min.get(i)) > 0 && rawData.compareTo(max.get(i)) < 0)
						tuple.record.add(i, rawData);
					else {
						throw new DBAppException("Wrong input, please enter a value for " + columnName + " between "
								+ min.get(i) + " and " + max.get(i));
					}
				}
				}  
			b=true;
			return b;
    	}
    
    
    
    
    public void updateTable(String str_TableName,
    		String str_ClusteringKeyValue,
    		Hashtable<String,Object> htbl_ColNameValue
    		)
    		throws DBAppException{
    	
    }
    public void deleteFromTable(String str_TableName,
    		Hashtable<String,Object> htbl_ColNameValue)
    		throws DBAppException{
    	
    }
    
    
    
    public void createIndex(String
    		str_TableName,
    		String[] str_arrColName) throws DBAppException{}
    
    
    
    public Iterator selectFromTable(SQLTerm[] arr_SQLTerms,
    		String[] str_arrOperators)
    		throws DBAppException{
    	//queries have to be more than operators
    	if(arr_SQLTerms.length<=str_arrOperators.length) {
    		System.out.println("queries have to be more than operators!");
    		throw new DBAppException();
    	}
    	
    	Hashtable<Integer,String> ht=new Hashtable<Integer,String>(); 
    	return (Iterator)ht;
    	
    }
    
    public static void main(String[] args) throws IOException, DBAppException 
    {
    	String strTableName = "Student";
    	DBApp dbApp = new DBApp( );
    	dbApp.init();
    	Hashtable htblColNameType = new Hashtable( );
    	htblColNameType.put("id", "java.lang.Integer");
    	htblColNameType.put("name", "java.lang.String");
    	htblColNameType.put("gpa", "java.lang.double");
    	
    	Hashtable htblColNameMin = new Hashtable( );
    	htblColNameMin.put("id", "0");
    	htblColNameMin.put("name", "A");
    	htblColNameMin.put("gpa", "6.0");
    	
    	Hashtable htblColNameMax = new Hashtable( );
    	htblColNameMax.put("id", "10000");
    	htblColNameMax.put("name", "ZZZZZZZZZZZ");
    	htblColNameMax.put("gpa", "0.7");
    	
    	dbApp.createTable( strTableName, "id", htblColNameType,htblColNameMin,htblColNameMax);
    	
    	
    	
    }
    
}
