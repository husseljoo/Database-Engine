import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class DBApp implements DBAppInterface{
	FileWriter csvWriter;
	
	public void init()
	{
		String currLine ="Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max\n";
		try 
		{
			csvWriter = new FileWriter("metadata.csv");
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
    	String columnFormat=""; //FORMAT TO BE INSERTED IN CSV FILE OF TABLE FOR CONSISTENCY
    	Vector<String> columnFormatArr=new Vector();
    	int pos = 0;
    	int counter = 0;
    	while(enumeration.hasMoreElements()) 
        {
        	String columnName= enumeration.nextElement();
        	String columnType=htbl_ColNameType.get(columnName);
        	String clusterKey=Boolean.toString(columnName.equals(str_ClusteringKeyColumn)); 
        	if(columnName.equals(str_ClusteringKeyColumn))
        		pos = counter;
        	String columnMin=htbl_ColNameMin.get(columnName);
        	String columnMax=htbl_ColNameMax.get(columnName);
        	
        	currLine+= str_TableName+","+columnName+","+columnType+","+clusterKey+","+"False,"+columnMin+","+columnMax+"\n";
        	columnFormatArr.add(columnName);
        	try 
        	{
				csvWriter.append(currLine);
			} 
        	catch (IOException e) 
        	{
				e.printStackTrace();
			}
        	currLine = "";
        	counter++;
        }
        try 
        {
			csvWriter.flush();
		} catch (IOException e) 
        {
			e.printStackTrace();
		}
        /***************************/
        //Once we create a Table, we create a folder/directory with its name inside the tables folder
        //it will contain all the pages of the table as well as a table_name.csv that contains info about
        // its page files (.class or .ser) i.e key range of page,bit indicating whether its full or not etc.
        Path path =Paths.get("/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName);
        try 
        {
            Files.createDirectories(path);
            
         } catch (Exception e) {
        	  e.printStackTrace();
            System.err.println("Failed to create directory!" + e.getMessage());

          }
        
        
        TableInfo tableInfo=new TableInfo();
        tableInfo.colOrder=columnFormatArr;
        tableInfo.clusteringKey=str_ClusteringKeyColumn;
        tableInfo.clusterKeyIndex = pos;
        serialize(tableInfo,path+"/"+"tableInfo.class");
    }
    public static void serialize(Object o,String path) {
//    	String filename = "/home/husseljo/Desktop/DB2Project/src/main/resources/testTuple.ser"; //testTuple.class also works?
        
        try
	        {   
        	//Saving of object in a file
	        FileOutputStream file = new FileOutputStream(path);
	        ObjectOutputStream out = new ObjectOutputStream(file);
	        out.writeObject(o);
	        out.close();
	        file.close();
	        
	        }
	          
	    catch(IOException ex)
	        {
	        ex.printStackTrace();
	        System.out.println("IOException is caught");
	        }

    }
    public static Object deserialize(String path) {
    	Object o=null;
    	try
        {   
            // Reading the object from a file
    		FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);
              
            // Method for deserialization of object
            try{o= in.readObject();}catch(Exception e) {}
              
            in.close();
            file.close();
            
            
        }
          
        catch(Exception e)
        {
        	e.printStackTrace();
        	System.out.println("IOException is caught");
        	
        }
    	return o;

    } 
    public void insertIntoTable(String str_TableName,
    		Hashtable<String,Object> htbl_ColNameValue)
    		throws DBAppException{
    
//    		boolean isValid=checkValidity(str_TableName,htbl_ColNameValue);
    		if(true) 
    		{
    			String path="/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName+"/tableInfo.class";
    			TableInfo tableInfo=(TableInfo)deserialize(path);
    			Tuple tuple=new Tuple();
    			String clusteringKey=tableInfo.clusteringKey;
    			
    			for(int i=0;i<tableInfo.colOrder.size();i++) 
    			{
    				tuple.record.add(htbl_ColNameValue.get(tableInfo.colOrder.get(i)));
    			}
    			Object keyValue=htbl_ColNameValue.get(clusteringKey);
    			if(tableInfo.pages.size()==0)
    			{
    				Page page = new Page();
    				page.insert(tuple);
    				Object[] pageInfo = {"page0",keyValue};
    				tableInfo.pages.add(pageInfo);
    				tableInfo.nonOverflowPageNum++;
    				String pathOfPage = "/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName+"/page0.class";
    				serialize(page,pathOfPage);
    				serialize(tableInfo,path);
    			}
    			else
    			{
    				int position=0;
    				String key =(String)keyValue.toString();
    				for(int i=0;i<tableInfo.pages.size();i++) 
    				{
    					if((compareTo(key,tableInfo.pages.get(i)[1].toString())<0)||(i==tableInfo.pages.size()-1 && compareTo(key,tableInfo.pages.get(i)[1].toString())>0))
    						{
    						position=i;
    						break;
    						}
    				}
    				
    				String pathOfPage = "/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName+"/"+tableInfo.pages.get(position)[0].toString() +".class";
    				Page page=(Page) deserialize(pathOfPage);
    					if (!page.isFull()) 
    					{
    						add(tuple, page,false,tableInfo.clusterKeyIndex,tableInfo,position);
    						
    						serialize(page,pathOfPage);
    						serialize(tableInfo,path);
    					}
    					else 
    					{
    						createNewPage(tuple,page,tableInfo,tableInfo.pages.get(position)[0].toString(),keyValue,str_TableName,position);
    						//change max in pages dynamically
    						//add in tableinfo new tuple,page
    						serialize(page,pathOfPage);
    						serialize(tableInfo,path);
    					}
    				}
    			}
    		}
    public static void createNewPage(Tuple tuple, Page page, TableInfo tableInfo, String pageName, Object keyValue, String str_TableName, int posOfCurrentPage)
    {
    	//no next page
     	if(tableInfo.pages.lastElement()[0].toString().equals(pageName))
    	{
    		if(compareTo(tableInfo.pages.lastElement()[1].toString(),keyValue.toString())>0)
    		{
    			
    			//last elem akbar, put in new page
    			Page newPage = new Page();
    			newPage.insert(page.tuples.lastElement());
    			Object[] pageInfo = {"page"+(tableInfo.nonOverflowPageNum),tableInfo.pages.get(tableInfo.pages.size()-1)[1]};//esmaha maynfa3sh yeb2a .size, what if feeh 0,0_A el mfrood 1 msh 2
    			tableInfo.pages.add(pageInfo);
    			tableInfo.nonOverflowPageNum++;
    			String pathOfPage = "/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName+"/"+pageInfo[0].toString() +".class";
    			serialize(newPage,pathOfPage);
    			
    			
    			add(tuple,page,true,tableInfo.clusterKeyIndex,tableInfo,posOfCurrentPage);
    		}
    		else
    		{
    			//tuple akbar
    			Page newPage = new Page();
    			newPage.insert(tuple);
    			Object[] pageInfo = {"page"+(tableInfo.nonOverflowPageNum),keyValue};
    			tableInfo.pages.add(pageInfo);
    			tableInfo.nonOverflowPageNum++;
    			String pathOfPage = "/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName+"/"+pageInfo[0].toString() +".class";
    			serialize(newPage,pathOfPage);
    		}
    			
    	}
    	else
    	{
    		int positionOfNextPage = posOfCurrentPage+1;
    		String pathOfPage = "/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName+"/"+tableInfo.pages.get(positionOfNextPage)[0].toString() +".class";
			Page nextPage=(Page) deserialize(pathOfPage);
    		//next page is full
    		if(nextPage.isFull())
    		{
    			Page overFlow = new Page();
    			overFlow.insert(page.tuples.lastElement());
    			String nameOfOverFlowPage = "";
    			if(Character.isDigit(pageName.charAt(pageName.length()-1)))
    			{
    				nameOfOverFlowPage = pageName + "_A";
    			}
    			else
    			{
    				char letter= pageName.charAt(pageName.length()-1);
    				char nextLetter=(char) (letter+1);
    				nameOfOverFlowPage = pageName.replace(letter, nextLetter);
    			}
    			Object[] pageInfo = {nameOfOverFlowPage,tableInfo.pages.get(posOfCurrentPage)[1]};
    			
    			
    			
    			tableInfo.pages.insertElementAt(pageInfo,positionOfNextPage);
    			String path = "/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName+"/"+nameOfOverFlowPage +".class";
    			serialize(overFlow,path);
    			add(tuple,page,true,tableInfo.clusterKeyIndex,tableInfo,posOfCurrentPage);
    		}
    		//next page is not full
    		else
    		{
    			add(page.tuples.lastElement(),nextPage,false,tableInfo.clusterKeyIndex,tableInfo,posOfCurrentPage+1);
    			add(tuple,page,true,tableInfo.clusterKeyIndex,tableInfo,posOfCurrentPage);
    			
    		}
    		serialize(nextPage,pathOfPage);
    	}
    }
    public static int compareTo(String s1, String s2)
    {
    	try
    	{
    		return Integer.compare(Integer.parseInt(s1), Integer.parseInt(s2));
    	}
    	catch(Exception e)
    	{
    		try
    		{
    			return Double.compare(Double.parseDouble(s1), Double.parseDouble(s2));
    		}
    		catch(Exception e1)
    		{
    			try
    			{
    			Date date1=new SimpleDateFormat("yyyy/MM/dd").parse(s1);
    			Date date2=new SimpleDateFormat("yyyy/MM/dd").parse(s2);
    			return date1.compareTo(date2);
    			}
    			catch(Exception e2)
    			{
    				return s1.compareTo(s2);
    			}
    		}
    	}
    }
    public static void add(Tuple tuple,Page page, boolean isTruncate,int pos,TableInfo tableInfo,int posOfCurrentPage) 
    {
    	Comparator<Tuple> c = new Comparator<Tuple>() {
            public int compareReal(Tuple t1,Tuple t2, int pos)
            {
                return compareTo(t1.record.get(pos).toString(),t2.record.get(pos).toString());
            }

			public int compare(Tuple t1, Tuple t2) {
				return compareReal(t1,t2,pos);
			}
        };
        if(isTruncate)
   		 page.tuples.remove(page.tuples.size()-1);
    	int position=Math.abs(-(Collections.binarySearch(page.tuples, tuple,c))-1);
    	page.tuples.insertElementAt(tuple, position);
    	tableInfo.pages.get(posOfCurrentPage)[1] = page.tuples.lastElement().record.get(pos);
    	
    }
   
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
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
//		for(int j=0;j<arrOfStr.length;j++) System.out.println( j+" :"+arrOfStr[j]);
//		
//		System.out.println(fieldNames.size());
//		System.out.println(dataTypes); 
//		System.out.println(min);       
//		System.out.println(max);       
		
		
		
		
		
		Enumeration<String> enumeration = htbl_ColNameValue.keys();
		Object rawData;
		boolean b=false;
		boolean columnExists=false;
		
		
		while (enumeration.hasMoreElements()) {
			String columnName = enumeration.nextElement();
			for (int i = 0; i < fieldNames.size(); i++) {
				if (columnName.equals(fieldNames.get(i))) {
					rawData = htbl_ColNameValue.get(columnName); // (String) htbl_ColNameValue.get(columnName);
					columnExists=true;}
				else 
					continue;
				if (dataTypes.get(i).equals("java.lang.Integer")) {
						int data;
						try {
							data =(Integer)rawData;
						} catch (Exception e) {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " of type integer");
						}
						if (data > Integer.parseInt(min.get(i)) && data < Integer.parseInt(max.get(i)))
								;//tuple.record.add(i, rawData.toString());
						else {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " between "
									+ min.get(i) + " and " + max.get(i));
						}
					} else if (dataTypes.get(i).equals("java.lang.Double")) {
						double data;
						try {
							data = (Double)rawData;
						} catch (Exception e) {
							System.out.println("Wrong input, please enter a value for " + columnName + " of type double");
							throw new DBAppException();
						}
						if (data > Double.parseDouble(min.get(i)) && data < Double.parseDouble(max.get(i)))
								;//tuple.record.add(i, rawData.toString());
						else {
							System.out.println("Wrong input, please enter a value for " + columnName + " between "
									+ min.get(i) + " and " + max.get(i));
							throw new DBAppException();
						}
					} else if (dataTypes.get(i).equals("java.util.Date")) {
						Date data;
						SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
						try {
							data = (Date)rawData;
						} catch (Exception e) {
							throw new DBAppException("Wrong input, please enter a value for " + columnName
									+ " of type date in the format " + '"' + "yyy-MM-dd" + '"');
						}
						try {
							if (data.after(formatter.parse(min.get(i))) && data.before(formatter.parse(max.get(i))))
									;//tuple.record.add(i, rawData.toString());
							else {
								throw new DBAppException("Wrong input, please enter a value between " + columnName + " between "
										+ min.get(i) + " and " + max.get(i));
							}
						} catch (Exception e) {
							e.printStackTrace();
							throw new DBAppException();
						}
					} 
					//CHECKING BOUNDARY MIN &MAX STILL NOT IMPLEMENTED CORRECTLY
//					else if ( rawData.valueOf(max.get(i)) >0 && ((String)rawData).compareTo(max.get(i)) < 0) //rawData.equals(min.get(i))
//						tuple.record.add(i, rawData.toString());
//					else {
//						throw new DBAppException("Wrong input, please enter a value for " + columnName + " between "
//								+ min.get(i) + " and " + max.get(i));
//					}
				}
//				if(!columnExists)
//					throw new DBAppException("Input Data invalid!"+columnName+" is not a field!");
//				columnExists=false;
			
				}  
			b=true;
			return b;
    	}
    
    public static void checkValidUpdated(String currentColumn,String columnType,String columnMinimum,String columnMaximum) {}
    public void updateTable(String str_TableName,
    		String str_ClusteringKeyValue,
    		Hashtable<String,Object> htbl_ColNameValue
    		)
    		throws DBAppException{
    	
    	Path pathTable =Paths.get("/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName);
    	if (!Files.exists(pathTable))
    		throw new DBAppException("Specified Table does not exist at all!");
    		
    	
    	//checkValidityOfInput to be implemented later
    	String path="/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName+"/tableInfo.class";
		TableInfo tableInfo=(TableInfo)deserialize(path);;
		String clusteringKey=tableInfo.clusteringKey;
		
		for(int i=0;i<tableInfo.colOrder.size();i++) 
		{
			if(!tableInfo.colOrder.get(i).equals(clusteringKey)){
				if(!htbl_ColNameValue.containsKey(tableInfo.colOrder.get(i)))
					throw new DBAppException("Wrong Hashtable Keys!");
				}
		}
		//if this code is reached then Hashtable contains correct column name values ---> check validity(type and range)
		
		BufferedReader objReader = null;
		String strCurrentLine;
		try {
			objReader = new BufferedReader(new FileReader("metadata.csv"));
			strCurrentLine = objReader.readLine();
		} catch (IOException e) {e.printStackTrace();}
		
		String[] arrOfStr=new String[7];
		String str_CurrentLine;
		int positionOfTable=0;
		
		try {
			for (int i = 0; (str_CurrentLine = objReader.readLine()) != null; i++) {
				arrOfStr = str_CurrentLine.split(",");
				
				if (arrOfStr[0].equals(str_TableName)) {
					break; //start of Table found in metadata file(prevent linearly searching for every column)
					}
				}
			System.out.println(str_CurrentLine);//DEBUGGER
			
			String currentColumn;
			String columnType;
			String columnMinimum;
			String columnMaximum;
			
			if(arrOfStr[1].equals(clusteringKey))//or arrOfStr[3]==true and parse it to Boolean clustering key in metadata
				currentColumn=clusteringKey;
			else 
				currentColumn=(String)htbl_ColNameValue.get(arrOfStr[1]); //gets value of current column
			
			columnType=arrOfStr[2];
			columnMinimum=arrOfStr[5];
			columnMaximum=arrOfStr[6];
			checkValidUpdated(currentColumn,columnType,columnMinimum,columnMaximum);
			
			
			
			while((str_CurrentLine = objReader.readLine()) != null ) {
					arrOfStr = str_CurrentLine.split(",");
					if (arrOfStr[0].equals(str_TableName)) {
						System.out.println(str_CurrentLine);//DEBUGGER
						if(arrOfStr[1].equals(clusteringKey))
							currentColumn=clusteringKey;
						else 
							currentColumn=(String)htbl_ColNameValue.get(arrOfStr[1]); //gets value of current column
						
						columnType=arrOfStr[2];
						columnMinimum=arrOfStr[5];
						columnMaximum=arrOfStr[6];
						checkValidUpdated(currentColumn,columnType,columnMinimum,columnMaximum);
						}
					else
						break;
				
			}
		}
		catch (IOException e) {
			e.printStackTrace();
			}
		
//		Now check whether str_ClusteringKeyValue exists at all in any page
		
		
		
		
		
		
		
		
		
		
		
		

		Tuple tuple=new Tuple();
		
		for(int i=0;i<tableInfo.colOrder.size();i++) 
		{	//create tuple to be inserted instead of existing tuple (updating it)
			if(tableInfo.colOrder.get(i).equals(clusteringKey))
				tuple.record.add(str_ClusteringKeyValue);
			else
				tuple.record.add(htbl_ColNameValue.get(tableInfo.colOrder.get(i)));
		}
		
		
		//get position of the page that contains tuple to be updated
		int pagePosition=-1;
		for(int i=0;i<tableInfo.pages.size();i++) 
		{
			if((compareTo(str_ClusteringKeyValue,tableInfo.pages.get(i)[1].toString())<=0))
				{ 
				pagePosition=i;
				break;
				}
		}
		if(pagePosition==-1)
			throw new DBAppException("str_ClusteringKeyValue does not exist at all(too big)!"); 
		
		String pageName=(String)tableInfo.pages.get(pagePosition)[0];
		
		if(pagePosition==-1)throw new DBAppException("no place");//should be redundant as we have a checker to be implemented above
		
		
		Comparator<Tuple> c = new Comparator<Tuple>() {
            public int compareReal(Tuple t1,Tuple t2, int pos)
            {
                return compareTo(t1.record.get(pos).toString(),t2.record.get(pos).toString());
            }

			public int compare(Tuple t1, Tuple t2) {
				return compareReal(t1,t2,tableInfo.clusterKeyIndex);
			}
        };
        String pagePath="/home/husseljo/Desktop/DB2Project/src/main/resources/data/"+str_TableName+"/"+pageName+".class";
        Page page=(Page)deserialize(pagePath);
        //get position of tuple to be updated
        int tuplePosition=Math.abs(-(Collections.binarySearch(page.tuples, tuple,c))-1)-1;
		
      //checks if the tuple we matched is actually the one with the str_ClusteringKeyValue
        
        if(!page.tuples.get(tuplePosition).record.get(tableInfo.clusterKeyIndex).equals(str_ClusteringKeyValue)){ 
			
        	throw new DBAppException("The ClusteringKeyValue you entered does not exist!"); 
        }
        
		System.out.println("Page position is: "+pagePosition);
		System.out.println("Tuple position is: "+tuplePosition);
		
		page.tuples.set(tuplePosition,tuple);
		serialize(page,pagePath);
		
		
		
		
		
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
    	if(arr_SQLTerms.length<=str_arrOperators.length)
    	{
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
    	htblColNameType.put("gpa", "java.lang.Double");
    	
    	Hashtable htblColNameMin = new Hashtable( );
    	htblColNameMin.put("id", "0");
    	htblColNameMin.put("name", "A");
    	htblColNameMin.put("gpa", "6.0");
    	
    	Hashtable htblColNameMax = new Hashtable( );
    	htblColNameMax.put("id", "10000");
    	htblColNameMax.put("name", "ZZZZZZZZZZZ");
    	htblColNameMax.put("gpa", "0.7");
    	
    	dbApp.createTable( strTableName, "id", htblColNameType,htblColNameMin,htblColNameMax);
    
    	//INSERTION INTO TABLES
    	Hashtable htbl_values = new Hashtable( );
    	htbl_values.put("id", 1);
    	htbl_values.put("name", "Sherif");
    	htbl_values.put("gpa", 4);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	htbl_values.put("id", 6);
    	dbApp.insertIntoTable("Student",htbl_values);

    	htbl_values.put("id",30);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id", 10);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	htbl_values.put("id", 3);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id", 2);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	
    	htbl_values.put("id", 22);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id", 4);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	htbl_values.put("id", 7);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id",5);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	TableInfo StudentTableInfo=(TableInfo)dbApp.deserialize("/home/husseljo/Desktop/DB2Project/src/main/resources/data/Student/tableInfo.class");
    	String studentPath="/home/husseljo/Desktop/DB2Project/src/main/resources/data/Student/";
    	
    	//Print all pages with their contents
    	
    	for (int i = 0; i < StudentTableInfo.pages.size(); i++) {
    		String s=(String)(StudentTableInfo.pages.get(i)[0]);
    		System.out.println("Tuples in "+s+":"+"  MAX VALUE IS "+((Integer)StudentTableInfo.pages.get(i)[1]));
    		Page page=(Page)deserialize(studentPath+"/"+s+".class");
    		for (int j = 0; j < page.tuples.size(); j++) {
    			System.out.println("tuple"+j+": "+page.tuples.get(j).record.toString());}
    		System.out.println();
    	}
    	
    	Hashtable htbl_TEST = new Hashtable( );
    	htbl_TEST.put("gpa","1.4");
    	htbl_TEST.put("name","Hossam");
    	
    	dbApp.updateTable("Student","30",htbl_TEST);
    	
    	System.out.println();
    	System.out.println("--------------------------------------------------------");
    	System.out.println();
    	
    	for (int i = 0; i < StudentTableInfo.pages.size(); i++) {
    		String s=(String)(StudentTableInfo.pages.get(i)[0]);
    		System.out.println("Tuples in "+s+":"+"  MAX VALUE IS "+((Integer)StudentTableInfo.pages.get(i)[1]));
    		Page page=(Page)deserialize(studentPath+"/"+s+".class");
    		for (int j = 0; j < page.tuples.size(); j++) {
    			System.out.println("tuple"+j+": "+page.tuples.get(j).record.toString());}
    		System.out.println();
    	}
    	
    	
    	
    	
    }
    }