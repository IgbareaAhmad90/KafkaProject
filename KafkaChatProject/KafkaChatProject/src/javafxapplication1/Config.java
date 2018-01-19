package javafxapplication1;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.Scanner;


public  class Config {
	 public static String getConfig(String Choose)
	 {		
	 		String result = null;
			Properties prop = new Properties();
			InputStream input = null;
			try {
				if(new File("config.properties").exists())
				{
				input = new FileInputStream("config.properties");
				// load a properties file
				prop.load(input);
				// get the property value and print it out
				result = String.valueOf(prop.getProperty(Choose));
				} 
				return result;

					
			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {
				if (input != null) {
					try {
						input.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			return result;
	 }
	
	 public static boolean setConfig() throws IOException {
		    int ServerPort ;
		    String IPAddress;
		    
			Properties prop = new Properties();
			OutputStream output = null;
			Scanner s = new Scanner(System.in);
			try {
				output = new FileOutputStream("config.properties");
				do {
				boolean _errormsg = false;
				if(_errormsg)
					System.out.print("Error ServerPort Not Valid\n Try Again");
				
				System.out.println("Enter ServerPort  for Example '4444' ");
				ServerPort = s.nextInt();
				_errormsg = true;
				}while(!validPort(ServerPort));
				System.out.println("set the properties Port = "+ ServerPort);

				do {
					boolean _errorMessage = false;
					if(_errorMessage)
						System.out.print("Error IPAddress Not Valid\n");
			
					System.out.println("Enter IPAddress for Example '192.1.1.2' ");
					IPAddress = s.nextLine();
					_errorMessage = true;
				}while(!validIP(IPAddress));
				System.out.println("set the properties IPAddress ="+ IPAddress);

				// set the properties value
				prop.setProperty("port", ServerPort+"");
				prop.setProperty("IPAddress", IPAddress);

				// save properties to project root folder
				prop.store(output, null);
				
				output.close();
				s.close();
				return true;	
			} catch (IOException io) {
				io.printStackTrace();
				s.close();
			}
			return false;
	 }
	    public static boolean validPort(int port){
	    	if(port <1024)
	    		return false;
	    	if(port >65000)
	    		return false;
	    	return true;
	    }
	 	public static boolean validIP (String ip) {
		    try {
		        if ( ip == null || ip.isEmpty() ) {
		            return false;
		        }

		        String[] parts = ip.split( "\\." );
		        if ( parts.length != 4 ) {
		            return false;
		        }

		        for ( String s : parts ) {
		            int i = Integer.parseInt( s );
		            if ( (i < 0) || (i > 255) ) {
		                return false;
		            }
		        }
		        if ( ip.endsWith(".") ) {
		            return false;
		        }

		        return true;
		    } catch (NumberFormatException nfe) {
		        return false;
		    }
		}
}
