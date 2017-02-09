import org.apache.log4j.Logger;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * Created by alper on 1/25/17.
 */
public class GMailSSL {
    Properties props;
    Session session;
    String username;
    private static final Logger logger = Logger.getLogger(GMailSSL.class);

    public GMailSSL() {
        props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");
        NotifierProperties np = new NotifierProperties();
        username = np.getGMailUsername();
        session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, np.getGMailPassword());
                    }
                });
    }

    void sendMail(String subject, String body) {
        if(username==null || username.matches(""))
        {
            logger.error("No username registered for GMail!");
            return;
        }
        try {

            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(username + "@gmail.com"));
            message.setRecipients(Message.RecipientType.TO,
                    InternetAddress.parse(username + "@gmail.com"));
            message.setSubject(subject);
            message.setText(body);

            Transport.send(message);
            logger.info("Mail sent with subject: " + subject);

        } catch (MessagingException e) {
            logger.error("GMail error! ", e);
            throw new RuntimeException(e);
        }
    }

    void sendNewProductMail(Product product) {
        String subject = "New Product Alert!";
        String body = "New product on: \n" + product.getTitle() + " - " + product.getUrl();
        sendMail(subject, body);
    }

    void sendModifiedProductMail(Product product, String modification) {
        String subject = "Modified Product Alert!";
        String body = modification + product.getTitle() + " - " + product.getUrl();
        sendMail(subject, body);
    }


}
