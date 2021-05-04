import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class DBApp implements DBAppInterface{
	static String pathOfFile="src/main/resources/data/";
	public void init()
	{
		FileWriter csvWriter;
		BufferedReader objReader = null;
		String strCurrentLine="";
		try {
			objReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			strCurrentLine = objReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
			}
		
		if(strCurrentLine != null)
			return;
		String currLine ="Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max\n";
		try 
		{
			
			csvWriter = new FileWriter("src/main/resources/metadata.csv");
			csvWriter.append(currLine);
			csvWriter.flush();
			csvWriter.close();
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
    	
    	FileWriter fileWrite;
		try {
			fileWrite = new FileWriter("src/main/resources/metadata.csv",true);
		
    	BufferedWriter csvWrite=new BufferedWriter(fileWrite);
    	
    	if(str_ClusteringKeyColumn==null||str_TableName==null)
    		throw new DBAppException("Cannot leave a field empty");
    	validateExistence(htbl_ColNameType,str_ClusteringKeyColumn);
    	Enumeration<String> enumeration = htbl_ColNameType.keys();
    	String currLine = "";
    	String columnFormat=""; //FORMAT TO BE INSERTED IN CSV FILE OF TABLE FOR CONSISTENCY
    	Vector<String> columnFormatArr=new Vector();
    	int pos = 0;
    	int counter = 0;
    	String clusterKeyType="";
    	while(enumeration.hasMoreElements()) 
        {
        	String columnName= enumeration.nextElement();
        	String columnType=htbl_ColNameType.get(columnName);
        	String clusterKey=Boolean.toString(columnName.equals(str_ClusteringKeyColumn)); 
        	if(columnName.equals(str_ClusteringKeyColumn))
        		{
        		pos = counter;
        		clusterKeyType = columnType;
        		}
        	String columnMin=htbl_ColNameMin.get(columnName);
        	String columnMax=htbl_ColNameMax.get(columnName);
        	String min=columnMin;
        	String max=columnMax;
        	
        	if(columnName==null||columnType==null||columnMin==null||columnMin==null)
        		throw new DBAppException("Cannot leave a field empty");
        	
        	String typeLowerCase=columnType.toLowerCase();
        	if(!(typeLowerCase.equals("java.lang.integer")||typeLowerCase.equals("java.lang.double")||typeLowerCase.equals("java.util.date")||typeLowerCase.equals("java.lang.string")))
        		throw new DBAppException("Incorrect data type");
        	
        	if(columnType.toLowerCase().equals("java.lang.integer")) {
        		try {
        			Integer.parseInt(min);
        			Integer.parseInt(max);
        		}
        		catch(Exception e) {
        			throw new DBAppException("Incorrect minimum or maximum data type");
        		}
        	}
        	else if(columnType.toLowerCase().equals("java.lang.double")) {
        		try {
        			Double.parseDouble(min);
        			Double.parseDouble(max);
        		}
        		catch(Exception e) {
        			throw new DBAppException("Incorrect minimum or maximum data type");
        		}
        	}
        	else if(columnType.toLowerCase().equals("java.util.date")) {
            	try {
            		Date date1=new SimpleDateFormat("yyyy-MM-dd").parse(min);
            		Date date2=new SimpleDateFormat("yyyy-MM-dd").parse(max);
            	}
            	catch(Exception e) {
            		throw new DBAppException("Incorrect minimum or maximum data type");
            	}
        	}
        	
        	if(compareTo(columnMin,columnMax)>0)
        		throw new DBAppException("the minimum should be less than the maximum");
        	
        	//validateExistence(htbl_ColNameType,columnName);
        	
        	if(columnType.equals("java.lang.String")) 
        	{        		
        		min='"'+columnMin+'"';
        		max='"'+columnMax+'"';
        	}
        	currLine+= str_TableName+","+columnName+","+columnType+","+clusterKey+","+"False,"+min+","+max+"\n";
        	columnFormatArr.add(columnName);
        	try 
        	{
				csvWrite.append(currLine);
			} 
        	catch (IOException e) 
        	{
				e.printStackTrace();
			}
        	currLine = "";
        	counter++;
        }
        
        /***************************/
        //Once we create a Table, we create a folder/directory with its name inside the tables folder
        //it will contain all the pages of the table as well as a table_name.csv that contains info about
        // its page files (.class or .ser) i.e key range of page,bit indicating whether its full or not etc.
        Path path =Paths.get(pathOfFile+str_TableName);
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
        tableInfo.clusterKeyType=clusterKeyType;
        serialize(tableInfo,path+"/"+"tableInfo.class");
        csvWrite.close();
		}
		catch(IOException e) {
			;
		}
    }
    public static void validateExistence(Hashtable<String,String> htbl, String columnName) throws DBAppException
    {
    	Enumeration<String> enumeration = htbl.keys();
    	int c=0;
    	while (enumeration.hasMoreElements()) {
    		String data=enumeration.nextElement();
    		if (data.equals(columnName))
    			c++;
    	}
    	if(c>1)
			throw new DBAppException("Column names need to be unique");
    	else if(c==0)
    		throw new DBAppException("Clustering key doesn't exist");
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
    
    			checkValidity(str_TableName,htbl_ColNameValue);
    			String path=pathOfFile+str_TableName+"/tableInfo.class";
    			TableInfo tableInfo=(TableInfo)deserialize(path);
    			Tuple tuple=new Tuple();
    			String clusteringKey=tableInfo.clusteringKey;
    			
    			for(int i=0;i<tableInfo.colOrder.size();i++) 
    			{
//    				if(i==tableInfo.clusterKeyIndex && tableInfo.clusterKeyType.toLowerCase().equals("java.lang.date"))
//    				{
//    					Date date = (Date)htbl_ColNameValue.get(tableInfo.colOrder.get(i));
//    					date.
//    				}
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
    				String pathOfPage = pathOfFile+str_TableName+"/page0.class";
    				serialize(page,pathOfPage);
    				serialize(tableInfo,path);
    			}
    			else
    			{
    				int position=0;
    				String key =(String)keyValue.toString();
    				for(int i=0;i<tableInfo.pages.size();i++) 
    				{
    					if((compareTo(keyValue,tableInfo.pages.get(i)[1])<0)||(i==tableInfo.pages.size()-1 && compareTo(keyValue,tableInfo.pages.get(i)[1])>0))
    						{
    						position=i;
    						break;
    						}
    				}
    				
    				String pathOfPage = pathOfFile+str_TableName+"/"+tableInfo.pages.get(position)[0].toString() +".class";
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
    			String pathOfPage = pathOfFile+str_TableName+"/"+pageInfo[0].toString() +".class";
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
    			String pathOfPage = pathOfFile+str_TableName+"/"+pageInfo[0].toString() +".class";
    			serialize(newPage,pathOfPage);
    		}
    			
    	}
    	else
    	{
    		int positionOfNextPage = posOfCurrentPage+1;
    		String pathOfPage = pathOfFile+str_TableName+"/"+tableInfo.pages.get(positionOfNextPage)[0].toString() +".class";
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
    			String path = pathOfFile+str_TableName+"/"+nameOfOverFlowPage +".class";
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
    public static int compareTo(Object s1, Object s2)
    {
    	if(s1 instanceof Date && s2 instanceof Date)
    	{
    		Date d1 = (Date)s1;
    		Date d2 = (Date)s2;
    		return d1.compareTo(d2);
    	}
    	try
    	{
    		return Integer.compare(Integer.parseInt(s1.toString()), Integer.parseInt(s2.toString()));
    	}
    	catch(Exception e)
    	{
    		try
    		{
    			return Double.compare(Double.parseDouble(s1.toString()), Double.parseDouble(s2.toString()));
    		}
    		catch(Exception e1)
    		{
    			try
    			{
    			SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd");
    			Date date1 = (Date)s1;
    		    Date date2 = (Date)s2;
    		    format.format(date1);
    		    format.format(date2);
    			return date1.compareTo(date2);
    			}
    			catch(Exception e2)
    			{
    				return s1.toString().compareTo(s2.toString());
    			}
    		}
    	}
    }
    public static void add(Tuple tuple,Page page, boolean isTruncate,int pos,TableInfo tableInfo,int posOfCurrentPage) 
    {
    	Comparator<Tuple> c = new Comparator<Tuple>() {
            public int compareReal(Tuple t1,Tuple t2, int pos)
            {
                return compareTo(t1.record.get(pos),t2.record.get(pos));
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
    public static void checkValidity(String str_TableName,Hashtable<String,Object> htbl_ColNameValue) throws DBAppException{
    	
    	Path path =Paths.get(pathOfFile+str_TableName);
    	if (!Files.exists(path)) {
    		throw new DBAppException("Specified Table does not exist");
    		}
    	
    	BufferedReader objReader = null;
		String strCurrentLine="";
		try {
			objReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			strCurrentLine = objReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
			}

		String[] arrOfStr = new String[0];
		boolean found = false;
		ArrayList<String> fieldNames = new ArrayList();
		ArrayList<String> dataTypes = new ArrayList();
		ArrayList<String> min = new ArrayList();
		ArrayList<String> max = new ArrayList();
		try {
			for (int i = 0; (strCurrentLine = objReader.readLine()) != null; i++) {
				arrOfStr = strCurrentLine.split(",");
				
				if (arrOfStr[0].equals(str_TableName)) {
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
		
		Enumeration<String> enumeration = htbl_ColNameValue.keys();
		Object rawData;
		boolean columnExists=false;
		
		while (enumeration.hasMoreElements()) {
			String columnName = enumeration.nextElement();
			for (int i = 0; i < fieldNames.size(); i++) {
				if (columnName.equals(fieldNames.get(i))) {
					rawData = htbl_ColNameValue.get(columnName); // (String) htbl_ColNameValue.get(columnName);
					columnExists=true;
					}
				else 
					continue;
				if (dataTypes.get(i).toLowerCase().equals("java.lang.integer")) {
						int data;
						try {
							data =Integer.parseInt(rawData.toString());
						} catch (Exception e) {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " of type integer");
						}
						if (data >= Integer.parseInt(min.get(i)) && data <= Integer.parseInt(max.get(i)))
								;
						else {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " between "
									+ min.get(i) + " and " + max.get(i));
						}
					} else if (dataTypes.get(i).toLowerCase().equals("java.lang.double")) {
						double data;
						try {
							data = Double.parseDouble(rawData.toString());
						} catch (Exception e) {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " of type double");
						}
						if (data >= Double.parseDouble(min.get(i)) && data <= Double.parseDouble(max.get(i)))
								;
						else {
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " between "
									+ min.get(i) + " and " + max.get(i));
						}
					} else if (dataTypes.get(i).toLowerCase().equals("java.util.date")) {
						
						try { 
							SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd");
							format.format(rawData);
							
						} catch (Exception e) {
							throw new DBAppException("Wrong input, please enter a value for " + columnName
									+ " of type date in the format " + '"' + "yyyy-MM-dd" + '"');
						}
						try {
							Date minLimit = new SimpleDateFormat("yyyy-MM-dd").parse(min.get(i));
							Date maxLimit = new SimpleDateFormat("yyyy-MM-dd").parse(max.get(i));
							
							if ((((Date) rawData).after(minLimit)) || rawData.equals(minLimit) && (((Date) rawData).before(maxLimit) || rawData.equals(maxLimit)))
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
					else 
					{	
						
						String minLimit = min.get(i).substring(1, min.get(i).length()-1).toLowerCase();
						String maxLimit = max.get(i).substring(1, max.get(i).length()-1).toLowerCase();
						if ( (rawData.toString().toLowerCase().compareTo(minLimit) < 0 || rawData.toString().toLowerCase().compareTo(maxLimit) > 0)) //rawData.equals(min.get(i)
							throw new DBAppException("Wrong input, please enter a value for " + columnName + " between "
								+ min.get(i) + " and " + max.get(i));
					}
				}
				if(!columnExists)
					throw new DBAppException("Input Data invalid! "+columnName+" is not a field!");
				columnExists=false;
				
			
				}  
			
    	}
    public void updateTable(String str_TableName,
    		String str_ClusteringKeyValue,
    		Hashtable<String,Object> htbl_ColNameValue
    		)
    		throws DBAppException{
    	
    	Path pathTable =Paths.get(pathOfFile+str_TableName);
    	if (!Files.exists(pathTable))
    		throw new DBAppException("Specified Table does not exist at all!");
    		
    	
    	String path=pathOfFile+str_TableName+"/tableInfo.class";
		TableInfo tableInfo=(TableInfo)deserialize(path);;
		String clusteringKey=tableInfo.clusteringKey;
		
		if( htbl_ColNameValue.size() != (tableInfo.colOrder.size()-1))
			throw new DBAppException("More Hashtable Keys than expected!");
		
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
			objReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			strCurrentLine = objReader.readLine(); //read first Header line (skip it to read data later)
		} catch (IOException e) {e.printStackTrace();}
		
		String[] arrOfStr=new String[7];
		String str_CurrentLine;
		int positionOfTable=0;
		
		String clusteringColumnType="";
		
		try {
			for (int i = 0; (str_CurrentLine = objReader.readLine()) != null; i++) {
				arrOfStr = str_CurrentLine.split(",");
				
				if (arrOfStr[0].equals(str_TableName)) {
					break; //start of Table found in metadata file(prevent linearly searching for every column)
					}
				}
			//System.out.println(str_CurrentLine);//DEBUGGER
			
			 
			
			String currentColumnValue;
			String columnType;
			String columnMinimum;
			String columnMaximum;
			
			//check whether each (by now determined to be valid)key column 
			
			if(arrOfStr[1].equals(clusteringKey)) {//or arrOfStr[3]==true and parse it to Boolean clustering key in metadata
				currentColumnValue=str_ClusteringKeyValue;
				clusteringColumnType=arrOfStr[2];
			}
			else 
				currentColumnValue=htbl_ColNameValue.get(arrOfStr[1])+""; //gets value of current column
			
			columnType=arrOfStr[2];
			columnMinimum=arrOfStr[5];
			columnMaximum=arrOfStr[6];
			checkValidUpdated(currentColumnValue,columnType,columnMinimum,columnMaximum);
			
			
			
			while((str_CurrentLine = objReader.readLine()) != null ) {
					if(str_CurrentLine==null)
						break;
					
					arrOfStr = str_CurrentLine.split(",");
					if (arrOfStr[0].equals(str_TableName)) {
					//	System.out.println(str_CurrentLine);//DEBUGGER
						if(arrOfStr[1].equals(clusteringKey)) {
							currentColumnValue=str_ClusteringKeyValue;
							clusteringColumnType=arrOfStr[2];}
						else 
							currentColumnValue=htbl_ColNameValue.get(arrOfStr[1])+""; //gets value of current column
						
						columnType=arrOfStr[2];
						columnMinimum=arrOfStr[5];
						columnMaximum=arrOfStr[6];
						checkValidUpdated(currentColumnValue,columnType,columnMinimum,columnMaximum);
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
		
		//create tuple to be inserted instead of existing tuple (updating it)
		for(int i=0;i<tableInfo.colOrder.size();i++) 
		{	
			if(tableInfo.colOrder.get(i).equals(clusteringKey) && tableInfo.clusterKeyType.toLowerCase().equals("java.util.date"))
				{
					int year = Integer.parseInt(str_ClusteringKeyValue.substring(0, 4));
					int month = Integer.parseInt(str_ClusteringKeyValue.substring(5, 7));
					int day = Integer.parseInt(str_ClusteringKeyValue.substring(8));
					
					Date date = new Date(year-1900,month-1,day);
					tuple.record.add(date);
				}
			else if(tableInfo.colOrder.get(i).equals(clusteringKey))
				tuple.record.add(str_ClusteringKeyValue);
			else
				tuple.record.add(htbl_ColNameValue.get(tableInfo.colOrder.get(i)));
			}
		
		
		//get position of the page that contains tuple to be updated
		int pagePosition=-1;
		for(int i=0;i<tableInfo.pages.size();i++) 
		{
			if((compareTo(tuple.record.get(tableInfo.clusterKeyIndex),tableInfo.pages.get(i)[1])<=0))
				{ 
				pagePosition=i;
				;
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
                return compareTo(t1.record.get(pos),t2.record.get(pos));
            }

			public int compare(Tuple t1, Tuple t2) {
				return compareReal(t1,t2,tableInfo.clusterKeyIndex);
			}
        };
        String pagePath=pathOfFile+str_TableName+"/"+pageName+".class";
        Page page=(Page)deserialize(pagePath);
        ;
        //get position of tuple to be updated
        int tuplePosition=Math.abs(-(Collections.binarySearch(page.tuples, tuple,c))-1)-1;
        if(clusteringColumnType.equals("java.util.String"))
        	tuplePosition+=1;
		
      //checks if the tuple we matched is actually the one with the str_ClusteringKeyValue
      //meaning if the to be updated tuple exists at all
        
        Object actualFoundTuple=page.tuples.get(tuplePosition).record.get(tableInfo.clusterKeyIndex);
        boolean clusteringKeyValueDoesExist=sameValue(str_ClusteringKeyValue,actualFoundTuple,clusteringColumnType);
        
        if(!clusteringKeyValueDoesExist){ 
        	
        //	System.out.println(page.tuples.get(tuplePosition).record.get(tableInfo.clusterKeyIndex).getClass());//DEBUGGER
        	//System.out.println("input ClusterinKeyValue -->"+str_ClusteringKeyValue.getClass());//DEBUGGER
        	
        	throw new DBAppException("The ClusteringKeyValue you entered does not exist!"); 
        }
        
        
        
        
	//	System.out.println("Page position is: "+pagePosition); //DEBUGGER
	//	System.out.println("Tuple position is: "+tuplePosition);//DEBUGGER
		
		page.tuples.set(tuplePosition,tuple);
		serialize(page,pagePath);
		
		
		
    }
    public void deleteFromTable(String str_TableName,
    		Hashtable<String,Object> htbl_ColNameValue)
    		throws DBAppException
    {
    	String path=pathOfFile+str_TableName+"/tableInfo.class";
		TableInfo tableInfo=(TableInfo)deserialize(path);
		String[] columnName=new String[htbl_ColNameValue.size()];
		Enumeration<String> enumeration = htbl_ColNameValue.keys();
		Object[] values=new Object[htbl_ColNameValue.size()];
		for(int i=0;i<htbl_ColNameValue.size();i++) {
			columnName[i]=enumeration.nextElement();
			values[i]=htbl_ColNameValue.get(columnName[i]);
		//	System.out.println("value:"+values[i]);
		}
		
		for(int i=0;i<tableInfo.pages.size();i++) {
			String pageName=(String)tableInfo.pages.get(i)[0];
			String pagePath=pathOfFile+str_TableName+"/"+pageName+".class";
	        Page page=(Page)deserialize(pagePath);
	        Boolean pageDeleted=false;
	        for(int j=0;j<page.tuples.size();j++) {
	        	boolean found=true;
	        	for(int k=0;k<columnName.length;k++) {
	        		if(!(page.tuples.get(j).record.get(tableInfo.colOrder.indexOf(columnName[k])).toString().equals(values[k].toString()))) {
	        			found=false;
	        			break;
	        		}	
	        	}
	        	if (found) {
	        		page.tuples.remove(j);
	        		j--;
	        		if(page.tuples.size()==0) {
	        			pageDeleted=true;
	        			Path pathOfPage=FileSystems.getDefault().getPath(pagePath);
	        			try {
							Files.delete(pathOfPage);
							tableInfo.pages.remove(i);
							i--;
							break;
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	        				
	        		}
	        	}
	        }
	        if(!pageDeleted) {
	        	tableInfo.pages.get(i)[1] = page.tuples.lastElement().record.get(tableInfo.clusterKeyIndex);
	        	serialize(page,pagePath);
	        }
		}
		serialize(tableInfo,path);
    	
    }
    public static void checkValidUpdated(String currentColumnValue,String columnType,String columnMinimum,String columnMaximum) throws DBAppException{
    	
    	if(columnMaximum.charAt(0)=='"'&&columnMaximum.charAt(columnMaximum.length()-1)=='"')
   		  columnMaximum=columnMaximum.substring(1,columnMaximum.length()-1);
   	  
   	  	if(columnMinimum.charAt(0)=='"'&&columnMinimum.charAt(columnMinimum.length()-1)=='"')
   		  columnMinimum=columnMinimum.substring(1,columnMinimum.length()-1);
   	  
   	  	if(currentColumnValue.charAt(0)=='"'&&currentColumnValue.charAt(currentColumnValue.length()-1)=='"')
   		  currentColumnValue=currentColumnValue.substring(1,currentColumnValue.length()-1);
    	switch(columnType.toLowerCase()) {
    	
		  case ("java.lang.integer"):
			  try {
				  System.out.println("currentColumnValue"+currentColumnValue);
				  System.out.println(currentColumnValue.getClass());
				  
				  Integer columnValue=Integer.parseInt(currentColumnValue);
				  System.out.println("columnValue is: "+columnValue);
				  
				  Integer minValue=Integer.parseInt(columnMinimum);
				  System.out.println("columnMinimum is: "+columnMinimum);
				  
				  Integer maxValue=Integer.parseInt(columnMaximum);
				  System.out.println("columnMaximum is: "+columnMaximum);
				  
				  if(columnValue<minValue||columnValue>maxValue)
					  throw new DBAppException("Input Value "+currentColumnValue+" is out of range!");
			  }
			  catch(Exception e){
				  throw new DBAppException("The column named "+currentColumnValue+" should be an Integer");
			  }
			  
			  break;
		  case "java.lang.double":
			  try {
				  Double columnValue=Double.parseDouble(currentColumnValue);
				  Double minValue=Double.parseDouble(columnMinimum);
				  Double maxValue=Double.parseDouble(columnMaximum);
				  if(columnValue<minValue||columnValue>maxValue)
					  throw new DBAppException("Input Value "+currentColumnValue+" is out of range!");
			  }
			  catch(Exception e){
				  e.printStackTrace();
				  throw new DBAppException("The column named "+currentColumnValue+" should be a Double");
			  }
		    break;
		  case "java.util.date":
			  try {
				  	int year = Integer.parseInt(columnMinimum.trim().substring(0, 4));
	                int month = Integer.parseInt(columnMinimum.trim().substring(5, 7));
	                int day = Integer.parseInt(columnMinimum.trim().substring(8));
	                Date minValue = new Date(year - 1900, month - 1, day);
	                
	                year = Integer.parseInt(columnMaximum.trim().substring(0, 4));
	                month = Integer.parseInt(columnMaximum.trim().substring(5, 7));
	                day = Integer.parseInt(columnMaximum.trim().substring(8));
	                Date maxValue = new Date(year - 1900, month - 1, day);
				  	
				  	boolean b=false;
				  	
				  	try {
				  		SimpleDateFormat Parser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
				  		Date columnValue = Parser.parse(currentColumnValue);
				  		if(columnValue.before(minValue)||columnValue.after(maxValue))
							  throw new DBAppException("Input Value "+currentColumnValue+" is out of range!");
				  	
				  	}
				  	catch(Exception e) {
				  		b=true;
				  	}
				  	
				  	if(b) {
				  		year = Integer.parseInt(currentColumnValue.trim().substring(0, 4));
		                month = Integer.parseInt(currentColumnValue.trim().substring(5, 7));
		                day = Integer.parseInt(currentColumnValue.trim().substring(8));
		                Date columnValue = new Date(year - 1900, month - 1, day);
		                if(columnValue.before(minValue)||columnValue.after(maxValue))
							  throw new DBAppException("Input Value "+currentColumnValue+" is out of range!");
				  	}
			      	
				  
	               

				
			  }
			  catch(Exception e){
				  throw new DBAppException("The column named "+currentColumnValue+" should be a Date");
			  }
			 break;
		  default:
			  
			  if(currentColumnValue.compareTo(columnMinimum)<0 || currentColumnValue.compareTo(columnMaximum)>0)
				  throw new DBAppException("Input Value "+currentColumnValue+" is wrong!");
				
		    
		}
		
    }
    public static boolean sameValue(String a,Object o,String type) {
		String s=o.toString();
		
		boolean b=false;
		try {
		switch(type.toLowerCase()) {
		  case "java.lang.integer":
		    Integer w=Integer.parseInt(a);
		    Integer w2=Integer.parseInt(s);
		    b=w.equals(w2); 
		    break;
		  case "java.lang.double":
		    Double d=Double.parseDouble(a);
		    Double d2=Double.parseDouble(s);
		    b=d.equals(d2);
		    break;
		  case ("java.util.date"):
			  if(o instanceof Date)
			  {
			  	int year = Integer.parseInt(a.substring(0, 4));
				int month = Integer.parseInt(a.substring(5, 7));
				int day = Integer.parseInt(a.substring(8));	
				Date date = new Date(year-1900,month-1,day);
				b=date.equals((Date)o);
			  }
		  break;
		  default:
		    b=a.equals(s);
		}
		}
		catch(Exception e) {}
		
		return b;
		
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
    public static void main(String[] args) throws IOException, DBAppException, ParseException 
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
    	htblColNameMin.put("gpa", "0");
    	
    	Hashtable htblColNameMax = new Hashtable( );
    	htblColNameMax.put("id", "10000");
    	htblColNameMax.put("name", "ZZZZZZZZZZZ");
    	htblColNameMax.put("gpa", "4");
    	
    	dbApp.createTable( strTableName, "id", htblColNameType,htblColNameMin,htblColNameMax);
    
    	//INSERTION INTO TABLES
    	Hashtable htbl_values = new Hashtable( );
    	htbl_values.put("id", 1);
    	htbl_values.put("name", "ZZZZZZZZZZZ");
    	htbl_values.put("gpa", 3);
    	
    	
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	htbl_values.put("id", 6);
    	dbApp.insertIntoTable("Student",htbl_values);

    	htbl_values.put("id", 8);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id", 10);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	htbl_values.put("id", 3);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id", 2);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	
    	htbl_values.put("id", 9);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id", 4);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	htbl_values.put("id", 7);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	htbl_values.put("id",5);
    	dbApp.insertIntoTable("Student",htbl_values);
    	
    	
    	TableInfo StudentTableInfo=(TableInfo)dbApp.deserialize("src/main/resources/data/courses/tableInfo.class");
    	String studentPath="src/main/resources/data/courses/";
    	
    	//Print all pages with their contents
    	
//    	for (int i = 0; i < StudentTableInfo.pages.size(); i++) {
//    		String s=(String)(StudentTableInfo.pages.get(i)[0]);
//    		System.out.println("Tuples in "+s+":"+"  MAX VALUE IS "+((Integer)StudentTableInfo.pages.get(i)[1]));
//    		Page page=(Page)deserialize(studentPath+"/"+s+".class");
//    		for (int j = 0; j < page.tuples.size(); j++) {
//    			System.out.println("tuple"+j+": "+page.tuples.get(j).record.toString());}
//    		System.out.println();
//    	}
    	
    	Hashtable row = new Hashtable( );
    	row.put("course_id", "1100");
        row.put("course_name", "bar");
        row.put("hours", 13);


      //  dbApp.updateTable(strTableName, "course_id", row);
    	
    	System.out.println();
    	System.out.println("--------------------------------------------------------");
    	System.out.println();
    	
    	
    	
    	for (int i = 0; i < StudentTableInfo.pages.size(); i++) 
    	{
    		String s=(String)(StudentTableInfo.pages.get(i)[0]);
    		System.out.println("Tuples in "+s+":"+"  MAX VALUE IS "+(StudentTableInfo.pages.get(i)[1]));
    		Page page=(Page)deserialize(studentPath+"/"+s+".class");
    		for (int j = 0; j < page.tuples.size(); j++) {
    			System.out.println("tuple"+j+": "+page.tuples.get(j).record.toString());}
    		System.out.println();
    	}
    	System.out.println("------------------------------");
    	Hashtable<String,Object> delete = new Hashtable<String,Object>( );
    	delete.put("id",8);
    	
    	
    	dbApp.deleteFromTable("Student",delete);
    	
    	
    	StudentTableInfo=(TableInfo)dbApp.deserialize("src/main/resources/data/Student/tableInfo.class");
    	
//    	htbl_values.put("id", 11);
//    	dbApp.insertIntoTable("Student",htbl_values);
//    	
//    	htbl_values.put("id", 12);
//    	dbApp.insertIntoTable("Student",htbl_values);
//    	

//    	
    	for (int i = 0; i < StudentTableInfo.pages.size(); i++) 
    	{
    		String s=(String)(StudentTableInfo.pages.get(i)[0]);
    		System.out.println("Tuples in "+s+":"+"  MAX VALUE IS "+(StudentTableInfo.pages.get(i)[1]));
    		Page page=(Page)deserialize(studentPath+"/"+s+".class");
    		for (int j = 0; j < page.tuples.size(); j++) {
    			System.out.println("tuple"+j+": "+page.tuples.get(j).record.toString());}
    		System.out.println();
    	}
    	
    	
    	
    	
    	}
    
    }