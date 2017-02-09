import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by alper on 1/13/17.
 */
abstract class MySaxParser extends DefaultHandler{
    InputStream is;
    StringBuilder nodeContent;
    String tmpValue;
    private static final Logger logger = Logger.getLogger( MySaxParser.class );
    MySaxParser(InputStream is) {
        this.is = is;
        nodeContent = new StringBuilder();
    }

    void parseProduct() throws IOException, SAXException {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        try {
            SAXParser parser = factory.newSAXParser();
            parser.parse(is, this);
        } catch (ParserConfigurationException e) {
            logger.error("Parser config error", e);
        }
    }

    @Override
    public void startElement(String s, String s1, String elementName, Attributes attributes) throws SAXException {
        nodeContent.setLength(0);
    }

    @Override
    abstract public void endElement(String s, String s1, String element) throws SAXException;

    @Override
    public void characters(char[] ac, int start, int length) throws SAXException {
        nodeContent.append(ac, start, length);
        tmpValue = nodeContent.toString();
    }
}
