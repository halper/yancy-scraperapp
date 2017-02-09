import com.sun.org.apache.xerces.internal.impl.io.MalformedByteSequenceException;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import utilities.ConnectionHandler;
import utilities.ProxyHolder;
import utilities.TwitterFactorySource;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.DAYS;

/**
 * Created by alper on 1/10/17.
 */
public class Main {
    private static HashMap<Long, Product> mainMapOfProducts;
    private static HashMap<String, ProductURL> dbProductURLsMap;
    private static HashMap<String, ProductURL> recentProductURLsMap;
    private static HashMap<String, ProductURL> previousProductURLsMap;
    private static final int TIMEOUT_SECS_FOR_XML_PARSING = 15;
    private static final int NUM_OF_DAYS_FOR_DB_REMOVAL = 180;
    private static final int NUM_OF_DAYS_FOR_RESTOCK_CONSIDERATION = 2;
    private static HashMap<String, Boolean> siteURLs;
    private static List<String> filters;
    private static boolean init;
    private static final Logger logger = Logger.getLogger(Main.class);
    private static AtomicInteger banCounter;
    private static ProxyHolder ph;

    public static void main(String[] args) {
        logger.info("System initialized!");
        banCounter = new AtomicInteger(0);
        ph = ProxyHolder.getInstance();
        ExecutorService executorService = Executors.newFixedThreadPool(500);
        init = false;
        previousProductURLsMap = new HashMap<>();
        while (true) {
            initFromDB(executorService);
            checkForModifiedProductURLsMap(executorService);
            removeNonExistingProductURLs();
            System.gc();
            previousProductURLsMap = new HashMap<>(recentProductURLsMap);
        }
    }

    private static void removeNonExistingProductURLs() {
        if (!hasAllSiteURLsParsed()) return;
        if (recentProductURLsMap.size() == dbProductURLsMap.size()) return;
        List<Long> productURLsToBeRemoved = dbProductURLsMap
                .values()
                .stream()
                .filter(productURL -> DAYS.between(productURL.getUpdatedAt(), LocalDateTime.now()) >= NUM_OF_DAYS_FOR_DB_REMOVAL)
                .map(ProductURL::getId)
                .collect(Collectors.toList());
        if (productURLsToBeRemoved.size() == 0) return;

        for (Product product : mainMapOfProducts.values()) {
            productURLsToBeRemoved.stream()
                    .filter(id -> product.getProductURL().getId() == id)
                    .forEach(id -> mainMapOfProducts.remove(product.getId()));
        }
        logger.info(productURLsToBeRemoved.size() + " productURLs found to be removed!");

        String sql = "DELETE FROM producturls WHERE id=?";
        try (Connection connection = DataSource.getInstance().getBds().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 0;
            for (Long id : productURLsToBeRemoved) {
                statement.setLong(1, id);
                statement.addBatch();
                if (i != 0 && i % 1000 == 0)
                    statement.executeBatch();
                i++;
            }
            statement.executeBatch();
        } catch (SQLException e) {
            logger.error("SQL Error!\n", e);
        }

    }

    private static boolean hasAllSiteURLsParsed() {
        for (Boolean parsed : siteURLs.values()) {
            if (!parsed) return false;
        }
        return true;
    }

    private static void initFromDB(ExecutorService executorService) {
        getSiteUrlsFromDB();
        getFiltersFromDB();
        getProductURLsFromDB();
        changeProxyIfRequired();
        getRecentProductURLS(executorService);
        if (!init) {
            init = true;
            List<String> productUrls = new ArrayList<>(dbProductURLsMap.keySet());
            mainMapOfProducts = getProducts(executorService, productUrls);
        }
    }

    private static void changeProxyIfRequired() {
        Random random = new Random(System.nanoTime());
        if(!init)
            changeProxy();
        else if(banned())
            changeProxy();
        else if (random.nextInt(100) % 7 == 0)
            changeProxy();
    }

    private static void changeProxy() {
        String sql = "SELECT host, port FROM proxies";
        try (Connection connection = DataSource.getInstance().getBds().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String host = rs.getString("host");
                int port = rs.getInt("port");
                ph.setProxy(host, port);
            }
        } catch (SQLException e) {
            logger.error("SQL Error for change proxy!\n", e);
        }
    }

    private static boolean banned() {
        return banCounter.get() >= siteURLs.size()*0.8;
    }

    private static void getRecentProductURLS(ExecutorService executorService) {
        recentProductURLsMap = getProductUrls(executorService);
    }

    private static void getProductURLsFromDB() {
        dbProductURLsMap = new HashMap<>();

        String sql = "SELECT * FROM producturls";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try (Connection connection = DataSource.getInstance().getBds().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                ProductURL pUrl = new ProductURL();
                pUrl.setId(rs.getLong("id"));
                pUrl.setUrl(rs.getString("url"));
                pUrl.setLastMod(rs.getString("lastMod"));
                String imageLoc = rs.getString("imageLoc");
                pUrl.setUpdatedAt(LocalDateTime.parse(rs.getString("updated_at"), formatter));

                if (!imageLoc.toLowerCase().matches("null"))
                    pUrl.setImageLoc(imageLoc);
                dbProductURLsMap.put(pUrl.getUrlString(), pUrl);
            }
        } catch (SQLException e) {
            logger.error("SQL Error!\n", e);
        }
    }

    private static void getFiltersFromDB() {
        filters = new ArrayList<>();

        String sql = "SELECT keyword FROM filters";
        try (Connection connection = DataSource.getInstance().getBds().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                filters.add(rs.getString("keyword"));
            }
        } catch (SQLException e) {
            logger.error("SQL Error!\n", e);
        }
    }

    private static void getSiteUrlsFromDB() {
        siteURLs = new HashMap<>();

        String sql = "SELECT url FROM sites";
        try (Connection connection = DataSource.getInstance().getBds().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                siteURLs.put(rs.getString("url"), false);
            }
        } catch (SQLException e) {
            logger.error("SQL Error!\n", e);
        }
    }

    private static void checkForModifiedProductURLsMap(ExecutorService executorService) {

        List<String> newProducts = new ArrayList<>();
        List<String> modifiedProducts = new ArrayList<>();

        for (String s : recentProductURLsMap.keySet()) {
            if (!dbProductURLsMap.containsKey(s)) {
                newProducts.add(s);
            } else if (!dbProductURLsMap.get(s).getLastMod().matches(recentProductURLsMap.get(s).getLastMod())) {
                modifiedProducts.add(s);
            } else if (previousProductURLsMap.size() > 0 && !previousProductURLsMap.containsKey(s)) {
                modifiedProducts.add(s);
            }
        }
        if (newProducts.size() > 0 || modifiedProducts.size() > 0) {
            if (newProducts.size() > 0) {
                logger.info(newProducts.size() + " new products found!");
                sendNotificationsAboutNewProductsAndPutThemIntoMap(executorService, newProducts);
                insertNewProductURLsToDB(newProducts);
            }
            if (modifiedProducts.size() > 0) {
                logger.info(modifiedProducts.size() + " possible modified products found!");
                checkForModifiedProductsNotifyAndUpdateProductsMap(executorService, modifiedProducts);
                updateModifiedProductsInDB(modifiedProducts);
            }
        }
    }

    private static void updateModifiedProductsInDB(List<String> modifiedProducts) {
        String sql = "UPDATE producturls SET imageLoc=?, lastMod=?, updated_at=? WHERE id=?";
        try (Connection connection = DataSource.getInstance().getBds().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 0;
            for (String s : modifiedProducts) {
                ProductURL newProductUrl = recentProductURLsMap.get(s);
                ProductURL oldProductUrl = dbProductURLsMap.get(s);

                SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
                String date = sdf.format(new Date());
                statement.setString(1, newProductUrl.getImageLoc() == null ? "NULL" : newProductUrl.getImageLoc().toString());
                statement.setString(2, newProductUrl.getLastMod());
                statement.setString(3, date);
                statement.setLong(4, oldProductUrl.getId());
                statement.addBatch();
                if (i != 0 && i % 1000 == 0)
                    statement.executeBatch();
                i++;
            }
            statement.executeBatch();
        } catch (SQLException e) {
            logger.error("SQL Error!\n", e);
        }
    }

    private static void insertNewProductURLsToDB(List<String> newProducts) {

        String sql = "INSERT INTO producturls (url, imageLoc, lastMod, created_at, updated_at) VALUES (?, ?, ?, ?, ?)";
        try (
                Connection connection = DataSource.getInstance().getBds().getConnection();

                PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 0;
            for (String s : newProducts) {
                ProductURL pUrl = recentProductURLsMap.get(s);
                statement.setString(1, pUrl.getUrlString());
                SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
                String date = sdf.format(new Date());
                statement.setString(2, pUrl.getImageLoc() == null ? "NULL" : pUrl.getImageLoc().toString());
                statement.setString(3, pUrl.getLastMod());
                statement.setString(4, date);
                statement.setString(5, date);
                statement.addBatch();
                if (i != 0 && i % 1000 == 0)
                    statement.executeBatch();
                i++;
            }
            statement.executeBatch();
        } catch (SQLException e) {
            logger.error("SQL Error!\n", e);
        }
    }

    private static void checkForModifiedProductsNotifyAndUpdateProductsMap(ExecutorService executorService, List<String> modifiedProducts) {
        HashMap<Long, Product> modifiedMap = getProducts(executorService, modifiedProducts);
        Runnable task = () -> {
            for (Long aLong : modifiedMap.keySet()) {
                String modifiedText = null;
                Product oldProduct = mainMapOfProducts.get(aLong);
                Product newProduct = modifiedMap.get(aLong);
                boolean canBeConsideredAsRestocked = false;
                try {
                    if (dbProductURLsMap.containsKey(newProduct.getUrl())) {
                        canBeConsideredAsRestocked = DAYS.between(dbProductURLsMap.get(newProduct.getUrl()).getUpdatedAt(), LocalDateTime.now()) >= NUM_OF_DAYS_FOR_RESTOCK_CONSIDERATION;
                    }
                } catch (NullPointerException n) {
                    logger.error("Null product found while tweeting modified products!", n);
                }
                if (canBeConsideredAsRestocked) modifiedText = "Restocked ";

                if (oldProduct != null) {
                    if (newProduct.getQuantity() != 0 && oldProduct.getPrice() > newProduct.getPrice()) {
                        modifiedText = "Reduced price ";
                    }
                    if ((newProduct.getQuantity() > 0 && oldProduct.getPrice() == 0 && newProduct.getPrice() > 0)) {
                        modifiedText = "Restocked ";
                    }
                }
                if (modifiedText != null) {
                    logger.info("Modified product found!" + newProduct.getTitle() + "-" + newProduct.getUrl());
                    sendTweet(newProduct, modifiedText);
                    sendModifiedProductMail(newProduct, modifiedText);
                }
                mainMapOfProducts.put(aLong, newProduct);
            }
        };
        Thread worker = new Thread(task);
        executorService.execute(worker);
    }

    private static void sendModifiedProductMail(Product product, String modification) {
        GMailSSL gs = new GMailSSL();
        gs.sendModifiedProductMail(product, modification);
    }

    private static void sendNotificationsAboutNewProductsAndPutThemIntoMap(ExecutorService executorService, List<String> newProducts) {
        Runnable task = () -> {
            HashMap<Long, Product> newProductsMap = getProducts(executorService, newProducts);
            int i = 0;
            for (Product product : newProductsMap.values()) {
                sendTweet(product, "New product ");
                sendNewProductMail(product);
                if (++i % 10 == 0) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        logger.error("Interrupted method 'sendNotificationsAboutNewProductsAndPutThemIntoMap'!\n", e);
                    }
                }
            }
            mainMapOfProducts.putAll(newProductsMap);
        };
        Thread worker = new Thread(task);
        executorService.execute(worker);
    }

    private static void sendNewProductMail(Product product) {
        GMailSSL gs = new GMailSSL();
        gs.sendNewProductMail(product);
    }

    private static HashMap<Long, Product> getProducts(ExecutorService executorService, List<String> productUrls) {
        Long start = System.currentTimeMillis();
        logger.info("Parsing products!");

        HashMap<Long, Product> productsMap = new HashMap<>();

        List<Future<Product>> products = new ArrayList<>();
        for (String url : productUrls) {
            if (!recentProductURLsMap.containsKey(url))
                continue;
            ProductURL productUrl = recentProductURLsMap.get(url);
            if (dbProductURLsMap.containsKey(url))
                productUrl = dbProductURLsMap.get(url);
            Future<Product> future = executorService.submit(new ProductXMLParser(productUrl));
            products.add(future);
        }
        for (int i = 0; i < products.size(); i++) {
            Future<Product> product = products.get(i);
            try {
                Product myProduct = product.get(TIMEOUT_SECS_FOR_XML_PARSING, TimeUnit.SECONDS);
                productsMap.put(myProduct.getId(), myProduct);
            } catch (InterruptedException | ExecutionException | NullPointerException e) {
                if (e instanceof NullPointerException)
                    logger.warn("NullPointerException for " + productUrls.get(i));
            } catch (TimeoutException e) {
                logger.warn("Timeout exception for " + productUrls.get(i));
            }
        }

        logger.info(productsMap.size() + " products parsed in " + (System.currentTimeMillis() - start) / 1000 + " seconds");
        return productsMap;
    }

    private static void sendTweet(Product myProduct, String statusText) {
        statusText = statusText + myProduct.getUrl() + " - " + myProduct.getTitle();
        sendTweet(statusText);
    }

    private static void sendTweet(String statusText) {
        Twitter twitter = TwitterFactorySource.getFactory().getInstance();
        try {
            twitter.updateStatus(statusText);
            logger.info("Tweet sent! " + statusText);
        } catch (TwitterException e) {
            if (e.toString().contains("code - 187")) {
                statusText = statusText.replaceAll(" \\(\\w+\\d+\\)", "");
                String status = statusText + getRandomString();
                sendTweet(status);
            } else
                logger.error("Twitter exception for tweet: " + statusText, e);
        }
    }

    private static String getRandomString() {
        String randomString = " (";
        final String alphabet = "QWERTYUIOPASDFGHJKLZXCVBNM";
        final int N = alphabet.length();

        Random r = new Random();

        for (int i = 0; i < 6; i++) {
            if (i < 4)
                randomString += alphabet.charAt(r.nextInt(N));
            else
                randomString += r.nextInt(10);
        }
        return randomString + ")";
    }

    private static HashMap<String, ProductURL> getProductUrls(ExecutorService executorService) {
        logger.info("Parsing product URLs!");
        Long start = System.currentTimeMillis();
        List<Future<HashMap<String, ProductURL>>> productsFutureList = new ArrayList<>();
        List<String> siteURLsList = new ArrayList<>(siteURLs.keySet());

        for (String url : siteURLsList) {
            Future<HashMap<String, ProductURL>> future = executorService.submit(new URLWorker(url));
            productsFutureList.add(future);
        }
        HashMap<String, ProductURL> productUrlsAndLastModsMap = new HashMap<>();
        for (int i = 0; i < productsFutureList.size(); i++) {
            Future<HashMap<String, ProductURL>> listFuture = productsFutureList.get(i);
            try {
                HashMap<String, ProductURL> productURLHashMap = listFuture.get(TIMEOUT_SECS_FOR_XML_PARSING, TimeUnit.SECONDS);
                if (productURLHashMap != null)
                    productUrlsAndLastModsMap.putAll(productURLHashMap);
                siteURLs.put(siteURLsList.get(i), true);
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Error!", e);
            } catch (TimeoutException e) {
                logger.warn("Cannot get a URL in " + TIMEOUT_SECS_FOR_XML_PARSING + " seconds! " + siteURLsList.get(i));
            } catch (NullPointerException n) {
                logger.warn("NullPointerException for " + siteURLsList.get(i));
            }
        }
        logger.info("Product URLs parsed in " + (System.currentTimeMillis() - start) / 1000 + " seconds");
        return productUrlsAndLastModsMap;
    }

    private static class URLWorker implements Callable {
        private URL url;

        URLWorker(String url) {
            try {
                url += (url.charAt(url.length()-1) != '/' ?
                        "/" : "") + "sitemap_products_1.xml";

                this.url = new URL(url);
            } catch (MalformedURLException e) {
                logger.error("Malformed URL! " + url, e);
            }
        }

        @Override
        public HashMap<String, ProductURL> call() throws Exception {
            HashMap<String, ProductURL> productUrlsAndLastMod = null;

            ConnectionHandler connectionHandler = new ConnectionHandler(url);
            HttpURLConnection connection = connectionHandler.getConnection();
            try {
                if (connectionHandler.initiateConnection()) {
                    productUrlsAndLastMod = getProductUrls(connection.getInputStream());
                }
            } catch (IOException e) {
                if (e instanceof MalformedByteSequenceException) {
                    logger.warn("MalformedByteException for " + url);
                } else{
                    logger.error(url + " IOexception!!!", e);
                    banCounter.getAndIncrement();
                }
                Thread.sleep(1000);
                if (!(e instanceof SSLException || e instanceof MalformedByteSequenceException))
                    call();
            } finally {
                connection.disconnect();
                connection = null;
            }

            return productUrlsAndLastMod;
        }

        private HashMap<String, ProductURL> getProductUrls(InputStream is) throws IOException, SAXException {
            ProductURLSaxParser sp = new ProductURLSaxParser(is);
            List<ProductURL> productURLs = sp.getProductURLList();
            HashMap<String, ProductURL> productUrlsAndLastModMap = new HashMap<>();
            for (ProductURL productURL : productURLs) {
                if (isInFilter(productURL)) {
                    productUrlsAndLastModMap.put(productURL.getUrl().toString(), productURL);
                }
            }
            return productUrlsAndLastModMap;
        }

        private boolean isInFilter(ProductURL productURL) {
            for (String filter : filters) {
                if (productURL.getUrl().getPath().contains(filter))
                    return true;
            }
            return false;
        }
    }


}
