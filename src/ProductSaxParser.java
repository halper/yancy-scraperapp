import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by alper on 1/13/17.
 */
public class ProductSaxParser extends MySaxParser {
    Product tmpProduct;
    boolean inVariants;
    boolean inProduct;
    Variant variant;
    List<Variant> variants;

    public ProductSaxParser(InputStream is) throws IOException, SAXException {
        super(is);
        inVariants = false;
        inProduct = false;
        parseProduct();
        is.close();
    }

    @Override
    public void startElement(String s, String s1, String elementName, Attributes attributes) throws SAXException {
        super.startElement(s, s1, elementName, attributes);
        if (elementName.equalsIgnoreCase("hash")) {
            inProduct = true;
            tmpProduct = new Product();
        }
        if (elementName.equalsIgnoreCase("variants")) {
            inVariants = true;
            inProduct = false;
            variants = new ArrayList<>();
        }
        if (elementName.equalsIgnoreCase("variant")) {
            variants.add(variant);
            variant = new Variant();
        }
    }

    @Override
    public void endElement(String s, String s1, String element) throws SAXException {
        if (element.equalsIgnoreCase("id")) {
            Long id = Long.parseLong(tmpValue);
            if (inVariants) {
                variant.setId(id);
            } else if(inProduct) {
                tmpProduct.setId(id);
            }
        }
        if (element.equalsIgnoreCase("title")) {
            String title = tmpValue;
            if (inVariants)
                variant.setTitle(title);
            else if(inProduct)
                tmpProduct.setTitle(title);
        }
        if (element.equalsIgnoreCase("product-type")) {
            tmpProduct.setType(tmpValue);
        }
        if (element.equalsIgnoreCase("vendor")) {
            tmpProduct.setVendor(tmpValue);
        }
        if(element.equalsIgnoreCase("price")){
            variant.setPrice(tmpValue);
        }
        if(element.equalsIgnoreCase("inventory-quantity")){
            variant.setQuantity(Integer.parseInt(tmpValue));
        }
        if(element.equalsIgnoreCase("variants")){
            inVariants = false;
        }
    }

    Product getProduct() {
        return tmpProduct;
    }
}
