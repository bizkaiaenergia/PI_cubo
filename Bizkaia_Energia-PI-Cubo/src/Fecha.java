import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;




public class Fecha {
	
	static String LOCALE = "us_US";
	
	public static String hora;
	public static String minutos;
	public static String dia;
	public static String año;
	public static String año_largo;	
	public static String mes;
	public static String mes_corto;
	
	
	public String getLOCALE() {
		return LOCALE;
	}

	public void setLOCALE(String lOCALE) {
		LOCALE = lOCALE;
	}

	public String getHora() {
		return hora;
	}

	public void setHora(String hora) {
		Fecha.hora = hora;
	}

	public String getMinutos() {
		return minutos;
	}

	public void setMinutos(String minutos) {
		Fecha.minutos = minutos;
	}

	public String getDia() {
		return dia;
	}

	public void setDia(String dia) {
		Fecha.dia = dia;
	}

	public String getAño() {
		return año;
	}

	public void setAño(String año) {
		Fecha.año = año;
	}

	public String getAño_largo() {
		return año_largo;
	}

	public void setAño_largo(String año_largo) {
		Fecha.año_largo = año_largo;
	}

	public String getMes() {
		return mes;
	}

	public void setMes(String mes) {
		Fecha.mes = mes;
	}

	public String getMes_corto() {
		return mes_corto;
	}

	public void setMes_corto(String mes_corto) {
		Fecha.mes_corto = mes_corto;
	}

	public String getString() {
		return año_largo + mes_corto + dia + hora + minutos;
	}
	
	/**
	 * @param fecha
	 */
	public void setFecha(Date fecha) {
		Format formatter = new SimpleDateFormat("HH");
		hora = formatter.format(fecha);
		formatter = new SimpleDateFormat("mm");
		minutos = formatter.format(fecha);
		formatter = new SimpleDateFormat("dd");
		dia = formatter.format(fecha);
		formatter = new SimpleDateFormat("yy");
		año = formatter.format(fecha);
		formatter = new SimpleDateFormat("yyyy");
		año_largo = formatter.format(fecha);
		formatter = new SimpleDateFormat("MM");
		mes_corto = formatter.format(fecha);
		formatter = new SimpleDateFormat("MMM",new Locale(LOCALE));
		mes= formatter.format(fecha);
	}
	
	public static Date getFecha() {
		return getFecha(0,0);
	}
	
	public static Date getFecha(int iDesplazamientoEnHoras) {
		return getFecha(0,iDesplazamientoEnHoras);
	}
	
	public static Date getFecha(int iDesplazamientoEnDias,int iDesplazamientoEnHoras) {
		//Partimos de la fecha actual
		Date fechaActual = new Date();
		//Y le sumanos/restamos días
		if (iDesplazamientoEnDias!=0) {
			GregorianCalendar gcCalendario = new GregorianCalendar ();
			gcCalendario.setTime(fechaActual);
			gcCalendario.add(Calendar.DAY_OF_YEAR,iDesplazamientoEnDias);
			fechaActual = gcCalendario.getTime();
		//Y le sumanos/restamos horas	
		} else if (iDesplazamientoEnHoras!=0) {
			GregorianCalendar gcCalendario = new GregorianCalendar ();
			gcCalendario.setTime(fechaActual);
			gcCalendario.add(Calendar.HOUR_OF_DAY,iDesplazamientoEnHoras);
			fechaActual = gcCalendario.getTime();
		}
		return fechaActual;
	}

}
