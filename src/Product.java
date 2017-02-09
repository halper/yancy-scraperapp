import java.util.ArrayList;
import java.util.List;

class Product extends ProductTree{
    private String vendor;
    private String type;
    private ArrayList<Variant> variants;
    private ProductURL productURL;

    Product() {
        variants = new ArrayList<>();
    }

    public void addVariant(Variant variant){
        variants.add(variant);
    }

    public void addVariant(List<Variant> variantList){
        variants.addAll(variantList);
    }

    public List<Variant> getVariants(){
        return variants;
    }

    String getType() {
        return type;
    }

    void setType(String type) {
        this.type = type;
    }

    String getUrl() {
        return productURL.getUrl().toString();
    }

    ProductURL getProductURL() {
        return productURL;
    }

    void setProductURL(ProductURL productURL) {
        this.productURL = productURL;
    }

    @Override
    public int getQuantity() {
        int quantity = 0;
        for (Variant variant : variants) {
            quantity += variant.getQuantity();
        }
        return quantity;
    }

    public String getVendor() {
        return vendor;
    }

    void setVendor(String vendor) {
        this.vendor = vendor;
    }

    @Override
    public double getPrice() {
        double price = 0;
        for (Variant variant : variants) {
            if(price < variant.getPrice())
                price = variant.getPrice();
        }
        return price;
    }

    @Override
    public String toString(){
        return getTitle() + " - " + getUrl();
    }


}
