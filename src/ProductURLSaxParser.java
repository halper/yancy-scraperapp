import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by alper on 1/13/17.
 */
class ProductURLSaxParser extends MySaxParser {
   private ProductURL productURL;
   private List<ProductURL> productURLList;

    ProductURLSaxParser(InputStream is) throws IOException, SAXException {
        super(is);
        productURLList = new ArrayList<>();
        parseProduct();
        productURLList.remove(0);
        is.close();
    }

    @Override
    public void startElement(String s, String s1, String elementName, Attributes attributes) throws SAXException {
        super.startElement(s, s1, elementName, attributes);
        if (elementName.equalsIgnoreCase("url")) {
            productURL = new ProductURL();
        }
    }

    List<ProductURL> getProductURLList() {
        return productURLList;
    }

    @Override
    public void endElement(String s, String s1, String element) throws SAXException {
        if (element.equalsIgnoreCase("url")) {
            if (productURL.getUrl() != null)
                productURLList.add(productURL);
        }
        if (element.equalsIgnoreCase("loc")) {
            productURL.setUrl(tmpValue);
        }
        if (element.equalsIgnoreCase("lastmod")) {
            productURL.setLastMod(tmpValue);
        }
        if (element.equalsIgnoreCase("image:loc")) {
            productURL.setImageLoc(tmpValue);
        }
    }
}
