import org.w3c.dom.Element;

/**
 * Created by alper on 1/11/17.
 */
abstract class ProductTree {
    private String title;
    private long id;

    ProductTree(){}

    public ProductTree(String title, long id) {
        this.title = title;
        this.id = id;
    }

    void setTitleAndId(Element element){
        if(!(element.getElementsByTagName("title").getLength() > 0)||
                !(element.getElementsByTagName("id").getLength() > 0))
            throw new NullPointerException();

        setTitle(element.getElementsByTagName("title").item(0).getTextContent());
        setId(Long.parseLong(element.getElementsByTagName("id").item(0).getTextContent()));
    }

    void setTitle(String title) {
        this.title = title;
    }

    void setId(long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    long getId() {
        return id;
    }

    public abstract int getQuantity();

    public abstract double getPrice();

}
