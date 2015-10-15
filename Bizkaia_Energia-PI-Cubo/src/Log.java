
import org.apache.log4j.Logger;

/**
 * Funcion para escribir los logs n consola o fichero.
 * 
 * Configuracion: log4j.properties
 * 
 * @author pgonzalez
 */

public class Log {

	static String titulo = "OCS";
	static Logger logger = Logger.getLogger(titulo);
	
	/* Metodo que hace un logeo de error de la aplicación */
    public static void error(String sMensaje) {
    	try {
    		//Logger logger = Logger.getLogger(sAplicacion);
    		//BasicConfigurator.configure();
    		logger.error(sMensaje);
    	} catch (Exception e) {
    		System.out.println("ERROR > "+sMensaje);
    	}
    }
    
	/* Metodo que hace un logeo de error de la aplicación */
    public static void excepcion(Exception ex) {
    	try {
	    	//Logger logger = Logger.getLogger(sAplicacion);
	    	//BasicConfigurator.configure();
	    	logger.error(ex.getMessage());  
    	} catch (Exception e) {
    		e.printStackTrace();   
    	}    	    	 
    }
    
	/* Metodo que hace un logeo de debugeo de la aplicación */
    public static void debug(String sMensaje) {
    	try {   	
	    	//Logger logger = Logger.getLogger(sAplicacion);
	    	//BasicConfigurator.configure();
	    	logger.debug(sMensaje);
			} catch (Exception e) {
				System.out.println("DEBUG > "+sMensaje);
			}
    }   

	/* Metodo que hace un logeo de debugeo de la aplicación */
    public static void info(String sMensaje) {
    	try {    	
	    	//Logger logger = Logger.getLogger(sAplicacion);
	    	//BasicConfigurator.configure();
    		logger = Logger.getLogger(titulo);
	    	logger.info(sMensaje);
			} catch (Exception e) {
				System.out.println("INFO > "+sMensaje);
			}    	
    }    
    
}
