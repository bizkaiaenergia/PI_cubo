import javax.mail.*;
import javax.mail.internet.*;

import java.io.UnsupportedEncodingException;
import java.util.*;


public class Notificacion {
	public void EnviaNotificacion (Propiedades oPropiedades)
	{
		EnviaNotificacion (oPropiedades, "");
	}
	public void EnviaNotificacion (Propiedades oPropiedades, String mensaje)
	{
        Properties props = new Properties();
        props.put("mail.smtp.host", oPropiedades.mail_host);
		props.put("mail.smtp.port", oPropiedades.mail_port);
		
        Session session = Session.getDefaultInstance(props, null);

        String msgBody =  mensaje;

        try {
            Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(oPropiedades.mail_sender, "Tarea importacion PI - CUBO"));
            
            String recipients [] = oPropiedades.mail_recipients.split(",");
            
            for (int i = 0; i < recipients.length; i++)
            	msg.addRecipient(Message.RecipientType.TO, new InternetAddress(recipients[i]));
            
            msg.setSubject(oPropiedades.mail_subject);
            msg.setText(msgBody);
            Transport.send(msg);

        } catch (AddressException e) {
            // ...
        } catch (MessagingException e) {
            // ...
        } catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
        

		
	}

}
