

import java.util.Hashtable;
import java.util.Vector;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;


/**
 * Funcion para la consulta y actualizacion de cualquier base de datos 
 * 
 * @author pgonzalez
 */

public class SQL {
	
	static String SENTENCIA_CORRECTA = "ok";
	static String SEPARADOR_SENTENCIAS = ":;";

	private static Connection conexion = null;
	
	public static void setConexion(Connection cConexion) {
		conexion = cConexion;
	}

	public static Connection getConexion(Propiedades oPropiedades) throws SQLException {
		if (conexion == null) {
			if (oPropiedades.isLoaded()) 
			{
				conexion = DriverManager.getConnection(oPropiedades.bbdd_out_host, oPropiedades.bbdd_out_user, oPropiedades.bbdd_out_password);
			}
			else 
				conexion = getConexion();
		} 
		return conexion;
	}
	
	public static Connection getConexion() throws SQLException {
		if (conexion == null) {
			Propiedades oPropiedades = new Propiedades();
			oPropiedades.load();	
			if (oPropiedades.isLoaded()) 
				conexion = DriverManager.getConnection(oPropiedades.bbdd_out_host, oPropiedades.bbdd_out_user, oPropiedades.bbdd_out_password);
		} 
		return conexion;
	}

	public static String obtenerPLSQL(String sSQL) {
		Connection cConexion = null;
		ResultSet rsResultado = null;
		String sResultado = null;		
		try {
			cConexion = getConexion();
			cConexion.setAutoCommit(false);
			sResultado = obtenerPLSQL(cConexion,sSQL);
			
		} catch (SQLException ex) {
			Log.excepcion(ex);
			sResultado = ex.getMessage();
		} finally {
			if (rsResultado != null) {
				try {
					rsResultado.close();
					rsResultado = null;
				} catch (SQLException excep) {
					Log.excepcion(excep);
					sResultado = excep.getMessage();
				}
			}
			
			try {
				if (!(cConexion.isClosed())) {
					try {
						cConexion.close();
					} catch (SQLException excep2) {
						Log.excepcion(excep2);
						sResultado = excep2.getMessage();
					}
				}
			} catch (SQLException excep) {
				Log.excepcion(excep);
				sResultado = excep.getMessage();
			}
		}

		return sResultado;
	}

	public static String obtenerPLSQL(Connection cConexion, String sSQL) {
		ResultSet rsResultado = null;
		String sResultado = null;
		try {
			Log.debug(sSQL);
			rsResultado = cConexion.createStatement().executeQuery(sSQL);
			if (rsResultado.next()) sResultado = rsResultado.getString(1);
		} catch (SQLException excep) {
			Log.excepcion(excep);
			sResultado = excep.getMessage(); 
		}
		finally {
			if (rsResultado != null) {
				try {
					rsResultado.close();
					rsResultado = null;
				} catch (SQLException excep) {
					Log.excepcion(excep);
					sResultado = excep.getMessage();
				}
			}
		}
		return sResultado;
	}
	
	/*
	 * Permite ejecutar sentencias SQL de seleccion.
	 * El valor lo devuelve en un vector con String con el resultado de la operacion y Hashtable de valores.
	 */
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Vector obtener(String sSQL) {
		Connection cConexion = null;
		ResultSet rsResultado = null;
		Vector vResultado = new Vector(2);
		String sResultado = new String();
		Hashtable tResultado = new Hashtable();
		
		vResultado.removeAllElements();
		tResultado.clear();

		try {
			cConexion = getConexion();
			vResultado = obtener(cConexion, sSQL);
			tResultado = (Hashtable) vResultado.elementAt(0);
			sResultado = (String) vResultado.elementAt(1);
			sResultado = SENTENCIA_CORRECTA;
		} catch (SQLException ex) {
			Log.excepcion(ex);
			sResultado = ex.getMessage();
		} finally {
			if (rsResultado != null) {
				try {
					rsResultado.close();
					rsResultado = null;
				} catch (SQLException excep) {
					Log.excepcion(excep);
					sResultado = excep.getMessage();
				}
			}
			try {
				if (!(cConexion.isClosed())) {
					try {
						cConexion.close();
					} catch (SQLException excep2) {
						Log.excepcion(excep2);
						sResultado = sResultado + excep2.toString();
					}
				}
			} catch (Exception excep) {
				Log.excepcion(excep);
				sResultado = excep.getMessage();
			}
		}

		vResultado.add(0, tResultado);
		vResultado.add(1, sResultado);

		return vResultado;
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Vector obtener(Connection cConexion, String sSQL) {
		
		ResultSet rsResultado = null;
		Vector vResultado = new Vector(2);
		String sResultado = new String();
		Hashtable tResultado = new Hashtable();
		
		vResultado.removeAllElements();
		tResultado.clear();

		try {
			Log.debug(sSQL);
			rsResultado = cConexion.createStatement().executeQuery(sSQL);
			ResultSetMetaData rsmt = rsResultado.getMetaData();
			int numerocolumnas = rsmt.getColumnCount();
			tResultado.put("f0c0", new Integer(numerocolumnas));
			// Insertamos en la fila 0 los nombres de las columnas.
			for (int numcolumnanombres = 1; numcolumnanombres <= numerocolumnas; numcolumnanombres++) {
				tResultado.put("f0c" + numcolumnanombres, rsmt.getColumnName(numcolumnanombres));
				//tResultado.put("f1c" + numcolumnanombres, rsmt.getColumnTypeName(numcolumnanombres));
			}
			// A partir de la fila 1 insertamos los resultados de la sentencia.
			for (int fila = 1; rsResultado.next(); fila++) {
				for (int columna = 1; columna <= numerocolumnas; columna++) {
					if (rsResultado.getString(columna) != null)
						tResultado.put(("f" + fila + "c" + columna), rsResultado.getString(columna));
					else
						tResultado.put(("f" + fila + "c" + columna), "");
				}
			}
			rsResultado.close();
			sResultado = SENTENCIA_CORRECTA;
		} catch (SQLException excep) {
			Log.excepcion(excep);
			sResultado =  excep.getMessage();
		}

		vResultado.add(0, tResultado);
		vResultado.add(1, sResultado);

		return vResultado;
	}
	
	
	//Funcion que obtiene el Id asociado al TAGNAME en la tabla PI_TAGS
	public static int obtenerID(Connection cConexion, int tagid) {
		
		ResultSet rsResultado = null;
		
		String sSQL = "SELECT Id FROM [BI].[dbo].[PI_TAGS] where id="+tagid;
		
		int id = -1;


		try {
			//Log.debug(sSQL);
			rsResultado = cConexion.createStatement().executeQuery(sSQL);
			
			while (rsResultado.next()) 
			{
				id = rsResultado.getInt("ID");
				//Log.debug("ID obtenido: "+id);
			}
			rsResultado.close();
			
		} catch (SQLException excep) {
			Log.excepcion(excep);
		}

		return id;
	}
	
	public static int obtenerID(Connection cConexion, String tagname, int tagid, String tagDescriptor, String tagUnits) {
		
		ResultSet rsResultado = null;
		
		String sSQL = "";
		if (tagDescriptor == null && tagUnits == null)
			sSQL = "SELECT Id FROM [BI].[dbo].[PI_TAGS] where tagname='"+tagname+"' AND Id = "+tagid;
		else if (tagDescriptor == null && !(tagUnits == null))
			sSQL = "SELECT Id FROM [BI].[dbo].[PI_TAGS] where tagname='"+tagname+"' AND Id = "+tagid+" AND engunits = '"+tagUnits+"'";
		else if (!(tagDescriptor == null) && tagUnits == null)
			sSQL = "SELECT Id FROM [BI].[dbo].[PI_TAGS] where tagname='"+tagname+"' AND Id = "+tagid+" AND descriptor = '"+tagDescriptor+"'";
		else
			sSQL = "SELECT Id FROM [BI].[dbo].[PI_TAGS] where tagname='"+tagname+"' AND Id = "+tagid+" AND descriptor = '"+tagDescriptor+"' AND engunits = '"+tagUnits+"'";
		
		int id = -1;


		try {
			//Log.debug(sSQL);
			rsResultado = cConexion.createStatement().executeQuery(sSQL);
			
			while (rsResultado.next()) 
			{
				id = rsResultado.getInt("ID");
				//Log.debug("ID obtenido: "+id);
			}
			rsResultado.close();
			
		} catch (SQLException excep) {
			Log.excepcion(excep);
		}

		return id;
	}
	
	
	/*
	 * Permite ejecutar sentencias SQL de actualizacion y e insercion.
	 * Autocommit está a falso y se mantienen gestión de transacciones.
	 */

	public static String actualizar(String sSQL) {
		Connection cConexion = null;
		String sResultado = new String();	
		try {
			cConexion = getConexion();
			cConexion.setAutoCommit(false);
			sResultado = actualizar(cConexion,sSQL);
		} catch (SQLException ex) {
			Log.excepcion(ex);
			sResultado = ex.getMessage();
		} finally {
			try {
				if (!(cConexion.isClosed()))
					cConexion.close();
			} catch (SQLException excep) {
				Log.excepcion(excep);
				sResultado = excep.getMessage();
			}
		}

		return sResultado;
	}

	public static String actualizar(Connection cConexion, String sSQL) {
		String sResultado = new String();
		try {
			cConexion.setAutoCommit(false);
			Statement stmtResultado = cConexion.createStatement();
			try {
				int cont = 0;
				String[] arrsSQL = sSQL.split(SEPARADOR_SENTENCIAS);
				for (int i = 0; i < arrsSQL.length; i++) {
					Log.debug(arrsSQL[i]);
					cont = cont + stmtResultado.executeUpdate(arrsSQL[i]);
				}
				cConexion.commit();
				sResultado = SENTENCIA_CORRECTA;
			} catch (SQLException sqle) {
				Log.excepcion(sqle);
				try {
					cConexion.rollback();
					sResultado = sqle.getMessage();
				} catch (SQLException sqlex) {
					Log.excepcion(sqlex);
					sResultado = sqlex.getMessage();
				}
			} finally {
				//Error al hacer el autocommit
				try {
					stmtResultado.close();
				} catch (SQLException stmte) {
					Log.excepcion(stmte);
					sResultado = stmte.getMessage();
				}
			}
		} catch (SQLException sqlex) {
			Log.excepcion(sqlex);
			sResultado = sqlex.getMessage();
		}
		return sResultado;
	}

}