package ru.kpfu.itis.group11501.popov.infosearch.parser;

import opennlp.tools.stemmer.snowball.SnowballStemmer;
import ru.stachek66.nlp.mystem.holding.Factory;
import ru.stachek66.nlp.mystem.holding.MyStem;
import ru.stachek66.nlp.mystem.holding.MyStemApplicationException;
import ru.stachek66.nlp.mystem.holding.Request;
import ru.stachek66.nlp.mystem.model.Info;
import scala.None;
import scala.Option;
import scala.collection.JavaConversions;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Bogdan Popov on 18.02.2019.
 */
public class LemmatizeAndAddToDatabase {

    private static final String SPLIT_PATTERN = "[\\p{Z}\\s.,;:!?'\"()/\\\\]+";
    private static final String RUSSIAN_WORD_REGEX = "^[а-яА-Я\\-]+$";
    private static final String STOP_WORDS_PATH = "stopwords-ru.txt";
    private static final Set<String> STOP_WORDS = new HashSet<>();
    private static final SnowballStemmer porterStemmer = new SnowballStemmer(SnowballStemmer.ALGORITHM.RUSSIAN);
    private static final MyStem myStemAnalyzer = new Factory("-igd --eng-gr --format json --weight").newMyStem("3.0", Option.empty()).get();

    static {
        File file = new File("D:\\repositories\\infosearch\\src\\main\\resources\\" + STOP_WORDS_PATH);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = reader.readLine();
            while (line != null) {
                STOP_WORDS.add(line);
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<String> getWordsFromText(final String text) {
        return Arrays.stream(text.split(SPLIT_PATTERN))
                .map(String::toLowerCase)
                .filter(word -> !word.equals("–"))
                .filter(LemmatizeAndAddToDatabase::isRussianWord)
                .filter(word -> !isStopWord(word))
                .collect(Collectors.toList());
    }

    public static boolean isStopWord(final String word) {
        return STOP_WORDS.contains(word);
    }

    public static boolean isRussianWord(final String word) {
        return word.matches(RUSSIAN_WORD_REGEX);
    }

    public static String lemmatiseWithPorter(final String word) {
        return porterStemmer.stem(word).toString();
    }

    public static String lemmatiseWithMyStem(final String word) {
        try {
            final Iterable<Info> result =
                    JavaConversions.asJavaIterable(
                            myStemAnalyzer
                                    .analyze(Request.apply(word))
                                    .info()
                                    .toIterable());
            for (final Info info : result) {
                return info.lex().isEmpty() ? info.initial() : info.lex().get();
            }
        }
        catch (MyStemApplicationException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static ResultSet findAll(String table) {
        try {
            Connection conn = ConnectionSingleton.getConnection();
            String query = "SELECT * FROM " + table;
            PreparedStatement ps = conn.prepareStatement(query);
            return ps.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void createWord(String table, String term, UUID articleId) {
        try {
            Connection conn = ConnectionSingleton.getConnection();
            String query = " insert into " + table + " (id, term, articles_id) values (?, ?, ?)";
            PreparedStatement ps = conn.prepareStatement(query);
            UUID wordId = UUID.randomUUID();
            ps.setObject(1, wordId);
            ps.setString(2, term);
            ps.setObject(3, articleId);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void uploadWordsFromArticleToDatabase() {
        try {
            ResultSet rs = findAll("articles");
            if (rs != null) {
                System.out.println("Find articles");
                while (rs.next()) {
                    String article = rs.getString("content");
                    String title = rs.getString("title");
                    String keywords = rs.getString("keywords");
                    UUID articleId = (UUID) rs.getObject("id");
                    System.out.println("Getting words from article " + title.toUpperCase().substring(0, 30));
                    List<String> words = getWordsFromArticle(article, title, keywords);
                    System.out.println("Lemmatizing with porter stemmer");
                    for (String word : words) {
                        word = lemmatiseWithPorter(word);
                        createWord("words_porter", word, articleId);
                    }
                    System.out.println("Lemmatizing with MyStem");
                    for (String word : words) {
                        word = lemmatiseWithMyStem(word);
                        createWord("words_mystem", word, articleId);
                    }
                }
            }
            else {
                System.out.println("No articles found on database");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static List<String> getWordsFromArticle(String article, String title, String keywords) {
        return Stream.of(article, keywords, title)
                .map(LemmatizeAndAddToDatabase::getWordsFromText)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        uploadWordsFromArticleToDatabase();
    }
}
