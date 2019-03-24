package ru.kpfu.itis.group11501.popov.infosearch.parser;

import javafx.util.Pair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Math.log;
import static java.lang.Math.sqrt;
import static ru.kpfu.itis.group11501.popov.infosearch.parser.LemmatizeAndAddToDatabase.getWordsFromText;

public class CosSearch {

    public static final int ARTICLE_COUNT = 30;

    public static Double cosMeasure(List<Double> firstVector, List<Double> secondVector) {
        double scalar = 0d;
        double firstLenght = 0d;
        double secondLenght = 0d;
        for (int i = 0; i < firstVector.size(); i++) {
            scalar += firstVector.get(i) * secondVector.get(i);
            firstLenght += firstVector.get(i) * firstVector.get(i);
            secondLenght += secondVector.get(i) * secondVector.get(i);
        }
        return scalar / sqrt(firstLenght) * sqrt(secondLenght);
    }

    public static List<Double> calculateIdfForRequest(List<String> requestWords, List<Map<String, Object>> articlesByTerm) {
        Map<String, Integer> termInAllArticles = new HashMap<>();
        List<Double> idfVector = new ArrayList<>();
        for (Map<String, Object> articleByTerm : articlesByTerm) {
            String term = (String) articleByTerm.get("term");
            termInAllArticles.merge(term, 1, (a, b) -> a + b);
        }
        for (String requestWord : requestWords) {
            Double idf = log((double) ARTICLE_COUNT / termInAllArticles.get(requestWord));
            idfVector.add(idf);
        }
        return idfVector;
    }

    public static Map<String, List<Double>> getArticleVectors(List<String> requestWords, List<Map<String, Object>> articlesByTerm) {
        Map<String, Map<String, Double>> termsIdfInArticle = new HashMap<>();
        for (Map<String, Object> articleByTerm : articlesByTerm) {
            String url = (String) articleByTerm.get("url");
            String term = (String) articleByTerm.get("term");
            Double tfIdf = (Double) articleByTerm.get("tf_idf");
            if (termsIdfInArticle.get(url) == null) {
                Map<String, Double> newMap = new HashMap<>();
                newMap.put(term, tfIdf);
                termsIdfInArticle.put(url, newMap);
            }
            else {
                termsIdfInArticle.get(url).put(term, tfIdf);
            }
        }
        Map<String, List<Double>> articleVectors = new HashMap<>();
        for (String article : termsIdfInArticle.keySet()) {
            List<Double> vector = new ArrayList<>();
            for (String requestWord : requestWords) {
                Double tfIdf = termsIdfInArticle.get(article).get(requestWord);
                if (tfIdf == null) {
                    vector.add(0d);
                }
                else {
                    vector.add(tfIdf);
                }
            }
            articleVectors.put(article, vector);
        }
        return articleVectors;
    }

    public static List<Map<String, Object>> findArticlesByTerms(List<String> requestWords) {
        try {
            Connection conn = ConnectionSingleton.getConnection();
            StringBuilder query = new StringBuilder("SELECT term, article_id, url, tf_idf FROM article_term " +
                    "JOIN articles ON (article_term.article_id=articles.id) " +
                    "JOIN terms_list ON (article_term.term_id=terms_list.term_id) WHERE ");
            for (String requestWord : requestWords) {
                query.append("term=? OR ");
            }
            query.delete(query.length() - 4, query.length());
            PreparedStatement ps = conn.prepareStatement(query.toString());
            for (int i = 1; i <= requestWords.size(); i++) {
                ps.setString(i, requestWords.get(i - 1));
            }
            ResultSet rs = ps.executeQuery();
            List<Map<String, Object>> result = new ArrayList<>();
            if (rs != null) {
                while (rs.next()) {
                    Map<String, Object> entry = new HashMap<>();
                    entry.put("term", rs.getString("term"));
                    entry.put("url", rs.getString("url"));
                    entry.put("tf_idf", rs.getDouble("tf_idf"));
                    result.add(entry);
                }
                return result;
            }
            else {
                System.out.println("No ResultSet");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<Pair<String, Double>> searchText(String searchText) {
        List<Pair<String, Double>> result = new ArrayList<>();
        List<String> requestWords = getWordsFromText(searchText);
        requestWords = requestWords.stream().map(LemmatizeAndAddToDatabase::lemmatiseWithPorter).collect(Collectors.toList());
        List<Map<String, Object>> articlesByTerm = findArticlesByTerms(requestWords);
        if (articlesByTerm != null) {
            List<Double> requestWordVector = calculateIdfForRequest(requestWords, articlesByTerm);
            Map<String, List<Double>> articleVectors = getArticleVectors(requestWords, articlesByTerm);
            for (String article : articleVectors.keySet()) {
                List<Double> articleVector = articleVectors.get(article);
                Double cos = cosMeasure(requestWordVector, articleVector);
                result.add(new Pair<>(article, cos));
            }
            result = result.stream().sorted(Comparator.comparing(Pair<String, Double>::getValue).reversed()).
                    limit(10).collect(Collectors.toList());
        }
        return result;
    }

    public static void main(String[] args) {
        List<Pair<String, Double>> result = searchText("Информационная безопасность");
        for (Pair<String, Double> resultEntry : result) {
            System.out.println("URL: " + resultEntry.getKey() + " cos: " + resultEntry.getValue());
        }
    }

}
