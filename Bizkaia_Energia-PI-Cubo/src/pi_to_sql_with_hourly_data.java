
import java.io.FileInputStream;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Stack;
import java.util.Vector;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Locale;

public class pi_to_sql_with_hourly_data {
	
	private static Propiedades oPropiedades = new Propiedades();
	
	private static Connection conexion = null;
	
	private static Hashtable<String,Stack<String[]>> tTags = new Hashtable<String, Stack<String[]>>();

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Log.info("Iniciada la importación ficheros sistema PI al SQL Server"); 
		int ejecucion = 0;
		//Cargar propiedades
		oPropiedades.load();
		
		if (oPropiedades.isLoaded()) 
		{
	
			try
			{
				
					
					//OBTENCION DE LOS DATOS DE PI
		
					//Obtenemos la hora de referencia de la medida
					Fecha objFecha = new Fecha();
					objFecha.setFecha(Fecha.getFecha()); 

					// HOURLY VALUES PLANT DATA
					// A veces los valores .TAG se quedan sin recalcular.
					// Por eso añado esta función para recalcular 2 horas hacia atrás antes exportar los valores
					// A continuación se obtiene el snapshot de los Tag indicados por properties
					
					String archivo_entrada_tags = oPropiedades.file_out_comandos_tags;
					String texto_entrada_tags = "@echo off\r\n@table pipoint\r\n@mode list\r\n@ostr Tag, PointId, descriptor, engunits\r\n@sigd 2\r\n@sele tag=*\r\n@ends\r\n";
										
					String archivo_entrada = oPropiedades.file_out_comandos;
					String texto_entrada = "@echo off\r\n@table pisnap\r\n@mode list\r\n@ostr tag, value, PointID, Time\r\n@sigd 2\r\n";
										
					for (int I = oPropiedades.tag_num; (--I) >= 0;) {
						
						if (oPropiedades.interaccionar_con_pi)
						{
							//Se lanza a PI la orden de recalcular por si algún valor no se actualizado, con esta operación se fuerza.
							recalcularTag(oPropiedades.tag[I],oPropiedades.cmd_pi_horas_previas);
						}
						//Líneas que indican los TAGS a leer, cargados del fichero de configuracion.
						texto_entrada += "@sele tag=" + oPropiedades.tag[I] + "\r\n@ends\r\n" ;
					}					
					
					Fichero.crear(archivo_entrada,texto_entrada);
					
					Fichero.crear(archivo_entrada_tags,texto_entrada_tags);
					
					String archivo_salida = oPropiedades.cms_out_path_temporal + oPropiedades.file_out_resultados + objFecha.getString() + ".dat";
					
					String cmd = oPropiedades.cmd_pi_configuracion + " < "+ archivo_entrada +" > " + archivo_salida;
					
					String archivo_salida_tags = oPropiedades.cms_out_path_temporal + oPropiedades.file_out_resultados_tags + objFecha.getString() + ".dat";
					String cmd_tags = oPropiedades.cmd_pi_configuracion + " < "+ archivo_entrada_tags +" > " + archivo_salida_tags;
					
					
					//archivo_salida contiene la salida del PI
					
					if (oPropiedades.interaccionar_con_pi)
					{
						ejecutar(cmd);
						ejecutar (cmd_tags);
					}
					
										
					try {
						abrirConexionBBDD();
						if (conexion!=null) {
							
							Log.info("Conexion con BBDD: OK");					
							//Guardamos los resultados del PI en SQL para el cubo de información
							guardarTags(archivo_salida_tags);
							guardarMedidas(archivo_salida);
							cerrarConexionBBDD();
							ejecucion = 1;
						}
						else
							Log.error("Conexion con BBDD ha devuelto null: ERROR");
							
					} catch (SQLException e) {
						e.printStackTrace();
						Log.excepcion(e);
					}
					
					
					
				
			} catch (IOException ioe) {
				ioe.printStackTrace();
				Log.excepcion(ioe);
			} catch (InterruptedException ie) {
				ie.printStackTrace();
				Log.excepcion(ie);
			}	 
	
		} 
		else 
		{
			Log.error("Propiedades de la aplicación no cargadas"); 
		}
		/*if (ejecucion == 1)
		{
			Notificacion objNotif = new Notificacion ();
			objNotif.EnviaNotificacion(oPropiedades, "Cargar de valores de PI ejecutada");
		}*/
	}
	
	/** Ejecuta comandos en PI
	 * @param sComando
	 */	
	
	private static void ejecutar(String sComando) throws IOException, InterruptedException {
		Runtime rt = Runtime.getRuntime();
		Process pr = rt.exec(sComando);
		BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));
		String line=null;
		while((line=input.readLine()) != null) {
			Log.debug(line); 
			int exitVal = pr.waitFor();
			Log.debug("Comando: " + sComando + " // Resultado: "+exitVal); 
		}
	}
	
	/** A veces los valores .TAG se quedan sin recalcular.
	 * 	Por eso añado esta función para recalcular 2 horas hacia atrás antes exportar los valores
	 * @param sNombreTag
	 */	

	private static void recalcularTag(String sNombreTag, int nHorasPrevias) throws IOException, InterruptedException {
		ejecutar(oPropiedades.cmd_pi_recalculo + " /ex=" +sNombreTag +",*-" + nHorasPrevias + "h,*");
	}

		
	/** Abrir conexion a cubo de Informacion
	 * @throws SQLException 
	 * @result true si la operacion ha sido correcta o false si ha sido erronea
	 */	
	
	private static void abrirConexionBBDD() throws SQLException {
		if (conexion == null) {
				conexion = SQL.getConexion(oPropiedades);
				Log.debug("Conexion con la base de datos establecida....");
		}
	}
	
	/** Cerrar conexion a cubo de informacion
	 * @throws SQLException 
	 * @result true si la operacion ha sido correcta o false si ha sido erronea
	 */	
	
	private static void cerrarConexionBBDD() throws SQLException {
		if (conexion != null) {
			conexion.close();
			conexion=null;
		}
	}
	
	
	/** Guearda en maximo las medidas de los tags
	 * @param sNombreTag
	 * @throws IOException 
	 */	

	private static void guardarMedidas(String sNombreArchivo) throws IOException {
		
		//Formato de los lados leidos de PI
		//TAGNAME,VALUE
		//Hay que introducir en la bbdd del cubo
		
		int error = 0;
		
		//Si no interaccionamos con PI leemos fichero sin fecha
		if (!oPropiedades.interaccionar_con_pi)
			sNombreArchivo = oPropiedades.cms_out_path_temporal + oPropiedades.file_out_resultados + ".dat";
		
		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(sNombreArchivo), "iso-8859-1"));
		
		String sLinea;
		boolean errores = false;
		
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS",Locale.ENGLISH);
		Date dateobj = new Date();
		Date objFechaActual = new Date();
		String fecha_actual=df.format(dateobj);
		
		//System.out.println(df.format(dateobj));
		
		//Creamos las sentencias para ejecutar en el SQL Server
		StringBuffer sbSQL = new StringBuffer(); 
		sbSQL.append(oPropiedades.bbdd_out_transaccion_init).append(SQL.SEPARADOR_SENTENCIAS);
		
		
		while ((sLinea = in.readLine())!=null) { 
			
			//Log.debug(sLinea);
			
			//Formato de la linea
			//X1:S1:GEN_MW,0.00,917,12-Feb-15 14:43:40.11401
			String tagname = sLinea.split(",")[0];
			String tagvalue = sLinea.split(",")[1];
			String tagId = sLinea.split(",")[2];
			String tagDate = sLinea.split(",")[3];
			
			String tagDateFormato = fecha_actual;
			// parse the date 12-Feb-15 14:50:38.85001
			//Unparseable date: "12-Feb-15 14:43:37.98901"
			//Hay fechas en este formato 12-Feb-15 14:53:00
			//Quitamos los milisegundos
			
			//DateFormat df2 = new SimpleDateFormat("dd-MMM-yy HH:mm:ss.SSSSS");
			DateFormat df2 = new SimpleDateFormat("dd-MMM-yy HH:mm:ss", Locale.ENGLISH);
			//Log.debug(Locale.getDefault().toString());
			
			//Log.debug(tagDate);
			
			if (tagDate.contains("."))
				tagDate = tagDate.split("\\.") [0];
			
			try
			{
				dateobj = df2.parse(tagDate); // works

				// now print the date
				tagDateFormato = df.format(dateobj);

			}
			catch (ParseException parseException )
			{
				Log.excepcion(parseException);		
				error = 1;
			}
						
			int id = SQL.obtenerID(conexion, Integer.parseInt(tagId));
			//Log.debug("Id obtenido: "+id);
			
			if (id == -1) // -1 no es un id valido, si se obtiene -1 no se introduden datos
			{
				//Es necesario volver a introducir valores en la tabla PI_TAGS ya que algún valor cargado no tiene su id correspondiente
				errores=true;
				Log.error("Se ha detectado ERROR ID para: "+sLinea);
			}
			else
			{
				
				//Log.debug(tagId+ " - " + tagvalue + " - " + fecha_actual + " - " + tagDateFormato);
				
				//Añadimos la transacción
				
				sbSQL.append("INSERT INTO [BI].[dbo].PI_VALUES (id, value, fecha_actual, fecha_adquisicion) VALUES ("+tagId+","+tagvalue+",CONVERT(datetime, '"+fecha_actual+"', 121),CONVERT(datetime, '"+tagDateFormato+"', 121))").append(SQL.SEPARADOR_SENTENCIAS);	
			
			}
			
			
			
		} 
		Log.debug("Errores detectados: "+errores);		
		in.close();
		sbSQL.append(oPropiedades.bbdd_out_transaccion_end).append(SQL.SEPARADOR_SENTENCIAS);
		//Log.debug(sbSQL.toString());
		
		String sResultado = SQL.actualizar(conexion,sbSQL.toString());	
		
		if(!sResultado.equals(SQL.SENTENCIA_CORRECTA)) 
		{
			Log.error("Sentencia: " + sbSQL.toString() + " // Resultado: " + sResultado );
			error = 1;
		}
		else
			Log.info("Inserción de datos OK // Resultado: " + sResultado );
		
		if (error != 0)
		{
			Notificacion objNotif = new Notificacion();
			objNotif.EnviaNotificacion(oPropiedades, "ERROR al insertar valores de PI en el cubo de informacion, es necesario revisar los logs de la ejecución");
		}
	}
	
	/** Guearda los Tags existentes en la tabla PI_TAGS
	 * @param sNombreTag
	 * @throws IOException 
	 */	

	private static void guardarTags(String sNombreArchivo) throws IOException {
		
		//Formato de los lados leidos de PI
		//Tag, PointId, descriptor, engunits
		//Hay que introducir en la bbdd del cubo, en la tabla PI_TAGS
		//Recorremos todos los TAGS y si detectamos que no está o que ha cambiado, hacemos el update o el insert, lo que corresponda
		int error = 0;
		
		if (!oPropiedades.interaccionar_con_pi)
			sNombreArchivo = oPropiedades.cms_out_path_temporal + oPropiedades.file_out_resultados_tags + ".dat";
		
		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(sNombreArchivo), "iso-8859-1"));
		
		String sLinea;
							
		//Creamos las sentencias para ejecutar en el SQL Server
		StringBuffer sbSQL = new StringBuffer(); 
		sbSQL.append(oPropiedades.bbdd_out_transaccion_init).append(SQL.SEPARADOR_SENTENCIAS);
		
		
		while ((sLinea = in.readLine())!=null) { 
				
			try
			{
				//tagDescriptor y tagUnits pueden ser null
				
				String tagname = sLinea.split(",")[0];
				int tagId = Integer.parseInt(sLinea.split(",")[1]);
				String tagDescriptor = null;
				String tagUnits = null;
				
				if (!sLinea.contains(",,"))
				{
					//Tiene descriptor
					tagDescriptor = sLinea.split(",")[2];
					
					//Si el último caracter no es coma, tiene unidades
					if  (!(sLinea.charAt(sLinea.length()-1) == ','))
					{
						tagUnits = sLinea.split(",")[3];
						
					}
					
				}
				else
				{
					//En este caso no tiene desciptor
					//Si el ultimo caracter no es coma, tiene unidades
					if  (!(sLinea.charAt(sLinea.length()-1) == ','))
					{
						tagUnits = sLinea.split(",")[2];
						Log.info("-D+U: "+sLinea);
					}
				}
				
				
				//if (tagUnits == null || tagDescriptor == null)
				//	Log.info("D o U son null : " + sLinea);
				
				
							
				int id = SQL.obtenerID(conexion, tagId);
				int id2 = SQL.obtenerID(conexion, tagname, tagId, tagDescriptor, tagUnits);
				
				// si id = -1, no existe entrada asociada
				// si id != -1 e id2 = -1, hay que actualizar los valores ya que algo ha cambiado
				
				//Log.debug("Id obtenido: "+id);
				
				if (id == -1) // -1 no es un id valido, si se obtiene -1 no se introduden datos
				{
					//Introducimos los valores en la tabla PI_TAGS
					Log.info("INSERT en PI_TAGS, linea con valor: "+sLinea);
					sbSQL.append("INSERT INTO [BI].[dbo].PI_TAGS (id, tagname, descriptor, engunits) VALUES ("+tagId+",'"+tagname+"','"+tagDescriptor+"','"+tagUnits+"')").append(SQL.SEPARADOR_SENTENCIAS);
				}
				else if ( id2 == -1) //Hay que hacer un update
				{
					
					//Log.debug(tagId+ " - " + tagvalue + " - " + fecha_actual + " - " + tagDateFormato);
					
					//Añadimos la transacción
					Log.info("UPDATE en PI_TAGS, linea con valor: "+sLinea);
					
					sbSQL.append("UPDATE [BI].[dbo].PI_TAGS SET tagname = '"+tagname+"', descriptor = '"+tagDescriptor+"', engunits = '"+tagUnits+"' WHERE id = "+tagId).append(SQL.SEPARADOR_SENTENCIAS);	
				
				}
				
				
			}
			catch (Exception e)
			{
				Log.excepcion(e);
				Log.error("EXCEPCION:" + sLinea);
				Log.error("EXCEPCION:" + e.getMessage() );
				System.out.println (e);
				
				error = 1;
			}
			
			
			
		} 
			
		in.close();
		sbSQL.append(oPropiedades.bbdd_out_transaccion_end).append(SQL.SEPARADOR_SENTENCIAS);
		
		//Log.debug(sbSQL.toString());
		
		String sResultado = SQL.actualizar(conexion,sbSQL.toString());	
		
		if(!sResultado.equals(SQL.SENTENCIA_CORRECTA)) 
		{
			Log.error("Sentencia: " + sbSQL.toString() + " // Resultado: " + sResultado );
			error=1;
		}
		else
			Log.info("Inserción de datos OK // Resultado: " + sResultado );
		
		if (error != 0)
		{
			Notificacion objNotif = new Notificacion();
			objNotif.EnviaNotificacion(oPropiedades, "ERROR al actualizar los TAGS de PI en el cubo de informacion, es necesario revisar los logs de la ejecución");
		}
		
		
	}
	
}
