import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;
import java.util.Vector;


public class Fichero {

	
	/** Crea un fichero con el contenido indicado
	 * @param sNombreFichero 
	 * @param sContenidoFichero 
	 */	
	
	public static void crear(String sNombreFichero, String sContenidoFichero) throws IOException {
		File f = new File(sNombreFichero);
		FileWriter fw;
		fw = new FileWriter(f);
		fw.write(sContenidoFichero);
		fw.close();
		Log.debug("Se ha creado el fichero " + sNombreFichero + ":\r\n" + sContenidoFichero);
	}
	
	/** Mueve el fichero del directorio actual al indicado
	 * @param sNombreFicheroSalida 
	 * @param sRutaSalida 
	 */	
	
	public static void mover(String sNombreFicheroSalida, String sRutaSalida) throws IOException {
		File file = new File(sNombreFicheroSalida);
		File dir = new File(sRutaSalida);	
		if (!file.renameTo(new File(dir,file.getName()))) {
			Log.error("No se ha podido mover el fichero " +   sNombreFicheroSalida + " a la dirección " + sRutaSalida);
		} else {
			Log.info("Se ha movido el fichero " + sNombreFicheroSalida + " a la dirección " + sRutaSalida + " con exito");
		}
	}
	
	/** Lee un fichero con el contenido indicado
	 * @param sNombreFichero 
	 * @return htContenidoFichero 
	 */	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Vector leer(String sNombreFichero) throws IOException {
		Vector out = new Vector(2);		
		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(sNombreFichero), "utf-8"));
		int nContador = 0;
		String sLinea;
		Hashtable<Integer,String> tLineas = new Hashtable<Integer,String>();
		while ((sLinea = in.readLine())!=null) { 
			tLineas.put(nContador, sLinea);
			nContador++;
		} 
		in.close();
		out.add(0, nContador);
		out.add(1, tLineas);
		return out;
	}
}
