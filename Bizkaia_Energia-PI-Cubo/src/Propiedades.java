

import java.io.*;
import java.util.Properties;

public class Propiedades {
	
	public static String ARCHIVO_PROPIEDADES="pi_to_sql.properties";
	public static boolean ARCHIVO_PROPIEDADES_CARGADO=false;
	
	//PROPIEDADES DE COMANDOS PI	
	public String cmd_pi_recalculo;
	public String cmd_pi_configuracion;
	public int cmd_pi_horas_previas=2;
	
	//PROPIEDADES DE TAGs
	public int tag_num=0;	
	public String[] tag={""};
	
	//PROPIEDADES DE PROGRAMACION DE SALIDA
	public String out_generacion_ultimo; 
	public int out_generacion_minuto; 
	public boolean out_generacion_bucle; 
		
	//PROPIEDADES DE NOMBRES DE FICHEROS
	public String file_out_comandos;
	public String file_out_resultados;
	public String file_out_comandos_tags;
	public String file_out_resultados_tags;
	
	//PROPIEDADES DE CMS DE SALIDA
	public String cms_out_path_temporal;
	public String cms_out_path_importacion; 

	//PROPIEDADES DE BBDD DE SALIDA
    public String bbdd_out_driver;
    public String bbdd_out_host;
    public String bbdd_out_user;
    public String bbdd_out_password;
    public String bbdd_out_transaccion_init;
    public String bbdd_out_transaccion_end;
    public String bbdd_out_transaccion_error;
    public String bbdd_out_current_date;
    public String mail_host;
    public String mail_port;
    public String mail_sender;
    public String mail_subject;
    public String mail_recipients;
    
    public boolean interaccionar_con_pi;
    
    /*
     * Lee del fichero de propiedades 
     */
    
    public void load() {
        Properties oPropiedades = new Properties();
		try {        
	        InputStream isPropiedades = this.getClass().getResourceAsStream(ARCHIVO_PROPIEDADES);
	        if (isPropiedades != null) {
    			oPropiedades.load(isPropiedades);
                //Configuracion de los comandos PI								
    			cmd_pi_recalculo = oPropiedades.getProperty("cmd_pi_recalculo");
                cmd_pi_configuracion = oPropiedades.getProperty("cmd_pi_configuracion");
                cmd_pi_horas_previas  = Integer.parseInt(oPropiedades.getProperty("cmd_pi_horas_previas"));
                
                //Configuracion de los tag de PI con los que se va a trabajar               
    			tag_num  = Integer.parseInt(oPropiedades.getProperty("tag_num"));
    			tag = new String[tag_num];  			
    			for (int I = tag_num; (--I) >= 0;) 
					tag[I] = oPropiedades.getProperty("tag_" + I);
    	
    			//Configuracion de la programación de la generacion de resultados de salida
                out_generacion_ultimo = oPropiedades.getProperty("out_generacion_ultimo");
                try {
                	out_generacion_minuto = Integer.parseInt(oPropiedades.getProperty("out_generacion_minuto"));
                } catch (NumberFormatException nfe) {
                	out_generacion_minuto = 0;
                }
                out_generacion_bucle = Boolean.parseBoolean(oPropiedades.getProperty("out_generacion_bucle"));
				
                //Configuracion de nombres de ficheros temporales
                file_out_comandos = oPropiedades.getProperty("file_out_comandos");
				file_out_resultados = oPropiedades.getProperty("file_out_resultados");
				file_out_comandos_tags = oPropiedades.getProperty("file_out_comandos_tags");
				file_out_resultados_tags = oPropiedades.getProperty("file_out_resultados_tags");
                
                //Configuracion de la conexion de CMS de salida	
				cms_out_path_temporal = oPropiedades.getProperty("cms_out_path_temporal");
                cms_out_path_importacion = oPropiedades.getProperty("cms_out_path_importacion");
                
                //Parametro que indica si se va a interaccionar con PI o se lee de un fichero concreto
                interaccionar_con_pi=Boolean.parseBoolean(oPropiedades.getProperty("interaccionar_con_pi"));
                
                //Configuracion de la conexion de BBDD de salida							
                bbdd_out_driver = oPropiedades.getProperty("bbdd_out_driver");
                bbdd_out_host = oPropiedades.getProperty("bbdd_out_host");
                bbdd_out_user = oPropiedades.getProperty("bbdd_out_user");
                bbdd_out_password = oPropiedades.getProperty("bbdd_out_password");
                bbdd_out_transaccion_init = oPropiedades.getProperty("bbdd_out_transaccion_init");
                bbdd_out_transaccion_end = oPropiedades.getProperty("bbdd_out_transaccion_end");
                bbdd_out_transaccion_error = oPropiedades.getProperty("bbdd_out_transaccion_error");
                bbdd_out_current_date = oPropiedades.getProperty("bbdd_out_current_date");
                
                //Configuraciones del envío de mails
                mail_host = oPropiedades.getProperty("mail_host");
                mail_port = oPropiedades.getProperty("mail_port");
                mail_sender = oPropiedades.getProperty("mail_sender");
                mail_subject = oPropiedades.getProperty("mail_subject");
                mail_recipients = oPropiedades.getProperty("mail_recipients");
                
            	isPropiedades.close();
            	ARCHIVO_PROPIEDADES_CARGADO=true;
            	debug();
			} 
		} catch (ArrayIndexOutOfBoundsException aioobe) {
	    	Log.error("Error al cargar las propiedades de " + ARCHIVO_PROPIEDADES + ". Error: Numero de tags no coincidentes");
	    	Log.excepcion(aioobe);    			
	    } catch (IOException ioe) {
	    	Log.error("Error al cargar las propiedades de " + ARCHIVO_PROPIEDADES + ". Error:" + ioe);
	    	Log.excepcion(ioe);
	    } finally {
	    	Log.info("Propiedades cargados correctamente de " + ARCHIVO_PROPIEDADES + ".");
	    	oPropiedades = null;
	    } 
    }
    
    /*
     * Muestra las propiedades cargadas del fichero
     */
    
    public void debug() {
    	//Configuracion de los comandos PI	
    	Log.debug("Propiedades cargadas:");
        Log.debug(" > cmd_pi_recalculo: "+cmd_pi_recalculo);
        Log.debug(" > cmd_pi_configuracion: "+cmd_pi_configuracion);
        Log.debug(" > cmd_pi_horas_previas: "+cmd_pi_horas_previas);

        
        //Configuracion de los tag de PI con los que se va a trabajar               
		Log.debug(" > tag_num: "+tag_num);
		
		for (int I = tag_num; (--I) >= 0;) 
			Log.debug(" > tag_"+ I +": " +  tag[I] );

        //Configuracion de la programación de la generacion de resultados de salida
        Log.debug(" > out_generacion_ultimo: "+out_generacion_ultimo);
        Log.debug(" > out_generacion_minuto: "+out_generacion_minuto);
        Log.debug(" > out_generacion_bucle: "+out_generacion_bucle);		
        
        //Configuracion de nombres de ficheros temporales
        Log.debug(" > file_out_comandos: "+file_out_comandos);
        Log.debug(" > file_out_resultados: "+file_out_resultados);
		
        //Configuracion de la conexion de CMS de salida	
        Log.debug(" > cms_out_path_temporal: "+cms_out_path_temporal);
        Log.debug(" > cms_out_path_importacion: "+cms_out_path_importacion);
        
        //Configuracion de la conexion de BBDD de salida							
        Log.debug(" > bbdd_out_driver: "+bbdd_out_driver);
        Log.debug(" > bbdd_out_host: "+bbdd_out_host);
        Log.debug(" > bbdd_out_user: "+bbdd_out_user);
        Log.debug(" > bbdd_out_password: "+bbdd_out_password);
        Log.debug(" > bbdd_out_transaccion_init: "+bbdd_out_transaccion_init);
        Log.debug(" > bbdd_out_transaccion_end: "+bbdd_out_transaccion_end);
        Log.debug(" > bbdd_out_transaccion_error: "+bbdd_out_transaccion_error);
        Log.debug(" > bbdd_out_current_date: "+bbdd_out_current_date);
        Log.debug(" > interaccionar_con_pi: "+interaccionar_con_pi);
    }
       
    /*
     * Muestra las propiedades se han cargado
     */
    
    public boolean isLoaded() {
    	return ARCHIVO_PROPIEDADES_CARGADO;
    }
	
}