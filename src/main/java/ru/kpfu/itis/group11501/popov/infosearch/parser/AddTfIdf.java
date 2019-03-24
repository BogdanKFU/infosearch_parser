package ru.kpfu.itis.group11501.popov.infosearch.parser;

/**
 * Created by Bogdan Popov on 11.03.2019.
 */
import javafx.util.Pair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static java.lang.Math.log;
import static ru.kpfu.itis.group11501.popov.infosearch.parser.LemmatizeAndAddToDatabase.findAll;

public class AddTfIdf {

    public static void insertTfIdf(UUID termId, UUID articleId, double tfIdf) {
        try {
            Connection conn = ConnectionSingleton.getConnection();
            String query = " UPDATE article_term SET tf_idf=? WHERE article_id=? AND term_id=?";
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setDouble(1, tfIdf);
            ps.setObject(2, articleId);
            ps.setObject(3, termId);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static UUID findTermId(String term) {
        try {
            Connection conn = ConnectionSingleton.getConnection();
            String query = "SELECT * FROM terms_list WHERE term=?";
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setString(1, term);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                rs.next();
                return (UUID) rs.getObject("term_id");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Map<Pair<String, UUID>, Double> calculateTfIdf(
            Map<Pair<String, UUID>, Integer> termPerArticle,
            Map<UUID, Integer> articleWordCount,
            Map<String, Integer> termInAllArticles, Integer articleCount){

        Map<Pair<String, UUID>, Double> termsTfIdf = new HashMap<>();
        for (Pair<String, UUID> termArticle : termPerArticle.keySet()) {
            double tfidf = ((double) termPerArticle.get(termArticle) / articleWordCount.get(termArticle.getValue()))
                    * log((double) articleCount / termInAllArticles.get(termArticle.getKey()));
            termsTfIdf.put(termArticle, tfidf);
        }
        return termsTfIdf;
    }

    public static void addTfIdf() {
        ResultSet rs = findAll("words_porter");
        try {
            if (rs != null) {
                Map<Pair<String, UUID>, Integer> termPerArticle = new HashMap<>();
                Map<UUID, Integer> articleWordCount = new HashMap<>();
                while (rs.next()) {
                    UUID articleId = (UUID) rs.getObject("articles_id");
                    String term = rs.getString("term");
                    Pair<String, UUID> pair = new Pair<>(term, articleId);
                    if (termPerArticle.get(pair) == null) {
                        termPerArticle.put(pair, 1);
                    }
                    else {
                        termPerArticle.put(pair, termPerArticle.get(pair) + 1);
                    }
                    if (articleWordCount.get(articleId) == null) {
                        articleWordCount.put(articleId, 1);
                    }
                    else {
                        articleWordCount.put(articleId, articleWordCount.get(articleId) + 1);
                    }
                }
                Integer articleCount = articleWordCount.keySet().size();
                Map<String, Integer> termInAllArticles = new HashMap<>();
                for (Pair<String, UUID> termArticle : termPerArticle.keySet()) {
                    String term = termArticle.getKey();
                    if (termInAllArticles.get(term) == null) {
                        termInAllArticles.put(term, 1);
                    }
                    else {
                        termInAllArticles.put(term, termInAllArticles.get(term) + 1);
                    }
                }
                Map<Pair<String, UUID>, Double> termsTfIdf = calculateTfIdf(termPerArticle, articleWordCount,
                        termInAllArticles, articleCount);
                for (Pair<String, UUID> termArticle : termsTfIdf.keySet()) {
                    UUID termId = findTermId(termArticle.getKey());
                    insertTfIdf(termId, termArticle.getValue(), termsTfIdf.get(termArticle));
                }
            }
            else {
                System.out.println("No words_porter found on database");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        addTfIdf();
    }
}
