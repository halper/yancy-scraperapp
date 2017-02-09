import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

class Variant extends ProductTree {
    private double price;
    private int quantity;

    public Variant(String title, long id) {
        super(title, id);
    }

    Variant(Element variantElement) {
        NodeList root = variantElement.getElementsByTagName("variant");
        Element element = (Element) root.item(0);
        setTitleAndId(element);
        if (element.getElementsByTagName("inventory-quantity").getLength() > 0)
            quantity = Integer.parseInt(element.getElementsByTagName("inventory-quantity").item(0).getTextContent());
        if (element.getElementsByTagName("price").getLength() > 0)
            price = Double.parseDouble(element.getElementsByTagName("price").item(0).getTextContent());
    }

    public Variant() {

    }

    public void setPrice(double price) {
        this.price = price;
    }

    void setPrice(String price){
        setPrice(Double.parseDouble(price));
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @Override
    public int getQuantity() {
        return quantity;
    }

    @Override
    public double getPrice() {
        return price;
    }
}
