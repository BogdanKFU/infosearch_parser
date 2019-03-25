package ru.kpfu.itis.group11501.popov.infosearch.parser;

import javafx.util.Pair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Math.log;
import static ru.kpfu.itis.group11501.popov.infosearch.parser.CosSearch.ARTICLE_COUNT;
import static ru.kpfu.itis.group11501.popov.infosearch.parser.CosSearch.findArticlesByTerms;
import static ru.kpfu.itis.group11501.popov.infosearch.parser.LemmatizeAndAddToDatabase.getWordsFromText;

public class Bm25search {

    public static final double K1 = 1.2;
    public static final double B = 0.75;

    public static Map<String, Long> countAllWordsInArticle() {
        try {
            Connection conn = ConnectionSingleton.getConnection();
            String query = "SELECT url, count(term_id) FROM article_term " +
                    "JOIN articles ON (article_term.article_id=articles.id) GROUP BY url";
            PreparedStatement ps = conn.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            if (rs != null) {
                Map<String, Long> result = new HashMap<>();
                while (rs.next()) {
                    result.put(rs.getString("url"), rs.getLong("count"));
                }
                return result;
            }
            else {
                System.out.println("No words in database");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Double averageCount(Map<String, Long> countAllWordsInArticle) {
        return countAllWordsInArticle.values().stream().mapToLong(value -> value).average().orElse(0D);
    }

    public static List<Double> calculateIdfForRequest(List<String> requestWords, List<Map<String, Object>> articlesByTerm) {
        Map<String, Integer> termInAllArticles = new HashMap<>();
        List<Double> idfVector = new ArrayList<>();
        for (Map<String, Object> articleByTerm : articlesByTerm) {
            String term = (String) articleByTerm.get("term");
            termInAllArticles.merge(term, 1, (a, b) -> a + b);
        }
        for (String requestWord : requestWords) {
            double idf = log((ARTICLE_COUNT - termInAllArticles.get(requestWord) + 0.5)
                    / (termInAllArticles.get(requestWord) + 0.5));
            idf = Math.max(idf, 0);
            idfVector.add(idf);
        }
        return idfVector;
    }

    public static Map<String, Map<String, Long>> countTermInArticle(List<Map<String, Object>> articlesByTerm) {
        Map<String, Map<String, Long>> result = new HashMap<>();
        for (Map<String, Object> articleByTerm : articlesByTerm) {
            String url = (String) articleByTerm.get("url");
            String term = (String) articleByTerm.get("term");
            Map<String, Long> articleMap = result.get(url);
            if (articleMap == null) {
                Map<String, Long> newMap = new HashMap<>();
                newMap.put(term, 1L);
                result.put(url, newMap);
            }
            else {
                articleMap.merge(term, 1L, (a, b) -> a + b);
            }
        }
        return result;
    }

    public static List<Pair<String, Double>> searchText(String searchText) {
        List<Pair<String, Double>> result = new ArrayList<>();
        List<String> requestWords = getWordsFromText(searchText);
        requestWords = requestWords.stream().map(LemmatizeAndAddToDatabase::lemmatiseWithPorter).collect(Collectors.toList());
        List<Map<String, Object>> articlesByTerm = findArticlesByTerms(requestWords);
        if (articlesByTerm != null) {
            List<Double> requestWordVector = calculateIdfForRequest(requestWords, articlesByTerm);
            Map<String, Map<String, Long>> countTermInArticle = countTermInArticle(articlesByTerm);
            Map<String, Long> countAllWordsInArticle = countAllWordsInArticle();
            if (countAllWordsInArticle != null) {
                Double averageCount = averageCount(countAllWordsInArticle);
                for (String article : countTermInArticle.keySet()) {
                    double score = 0;
                    for (String requestWord : requestWords) {
                        int index = requestWords.indexOf(requestWord);
                        double termFreq;
                        if (countTermInArticle.get(article).get(requestWord) == null) {
                            termFreq = 0;
                        }
                        else {
                            termFreq = (double) countTermInArticle.get(article).get(requestWord) / countAllWordsInArticle.get(article);
                        }
                        score += requestWordVector.get(index)
                                * (termFreq * (K1 + 1))
                                / (termFreq + K1 * (1 - B + B * countAllWordsInArticle.get(article) / averageCount));
                    }
                    result.add(new Pair<>(article, score));
                }
            }
        }
        result = result.stream().sorted(Comparator.comparing(Pair<String, Double>::getValue).reversed()).
                limit(10).collect(Collectors.toList());
        return result;
    }

    public static void main(String[] args) {
        List<Pair<String, Double>> result = searchText("Facebook хранила пароли миллионов пользователей в открытом доступе");
        for (Pair<String, Double> resultEntry : result) {
            System.out.println("URL: " + resultEntry.getKey() + " score: " + resultEntry.getValue());
        }
    }

}
