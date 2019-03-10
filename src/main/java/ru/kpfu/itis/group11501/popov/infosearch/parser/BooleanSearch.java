package ru.kpfu.itis.group11501.popov.infosearch.parser;

/**
 * Created by Bogdan Popov on 04.03.2019.
 */

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static ru.kpfu.itis.group11501.popov.infosearch.parser.LemmatizeAndAddToDatabase.getWordsFromText;
import static ru.kpfu.itis.group11501.popov.infosearch.parser.LemmatizeAndAddToDatabase.findAll;
import static ru.kpfu.itis.group11501.popov.infosearch.parser.LemmatizeAndAddToDatabase.lemmatiseWithPorter;

public class BooleanSearch {

    public static ResultSet findArticlesByText(String text) {
        try {
            Connection conn = ConnectionSingleton.getConnection();
            String query = "SELECT url FROM terms_list " +
                    "JOIN article_term ON (article_term.term_id=terms_list.term_id) " +
                    "JOIN articles ON (article_term.article_id=articles.id) " +
                    "WHERE terms_list.term=?";
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setString(1, text);
            return ps.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<String> searchText(String searchText) {
        List<String> words = getWordsFromText(searchText);
        List<List<String>> termArticles = new ArrayList<>();
        for (String word : words) {
            String lemm = lemmatiseWithPorter(word);
            ResultSet rs = findArticlesByText(lemm);
            List<String> articles = new ArrayList<>();
            try {
                while (rs.next()) {
                    articles.add(rs.getString(1));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            termArticles.add(articles);
        }
        Collections.sort(termArticles, (o1, o2) -> o1.size() - o2.size());
        List<String> result = termArticles.iterator().next();
        for (List<String> list : termArticles) {
            result = intersect(result, list);
            if (result.isEmpty()) {
                break;
            }
        }
        return result;
    }

    public static List<String> intersect(List<String> list1, List<String> list2) {
        return list1.stream()
                .filter(list2::contains)
                .collect(Collectors.toList());
    }

    public static UUID createTerm(String term) {
        try {
            Connection conn = ConnectionSingleton.getConnection();
            String query = " insert into terms_list (term_id, term) values (?, ?)";
            PreparedStatement ps = conn.prepareStatement(query);
            UUID termId = UUID.randomUUID();
            ps.setObject(1, termId);
            ps.setString(2, term);
            ps.execute();
            return termId;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void createArticleTerm(UUID termId, UUID articleId) {
        try {
            Connection conn = ConnectionSingleton.getConnection();
            String query = " insert into article_term (article_id, term_id) values (?, ?)";
            PreparedStatement ps = conn.prepareStatement(query);
            ;
            ps.setObject(1, articleId);
            ps.setObject(2, termId);
            ps.execute();
        } catch (SQLException ignored) {

        }
    }

    public static void addTermsToDatabase() {
        ResultSet rs = findAll("words_porter");
        try {
            if (rs != null) {
                Map<String, List<UUID>> termArticles = new HashMap<>();
                while (rs.next()) {
                    UUID articleId = (UUID) rs.getObject("articles_id");
                    String term = rs.getString("term");
                    if (termArticles.get(term) == null) {
                        List<UUID> list = new ArrayList<>();
                        list.add(articleId);
                        termArticles.put(term, list);
                    } else {
                        termArticles.get(term).add(articleId);
                    }
                }
                for (String term : termArticles.keySet()) {
                    UUID termID = createTerm(term);
                    for (UUID articleId : termArticles.get(term)) {
                        createArticleTerm(termID, articleId);
                    }
                }
            } else {
                System.out.println("No words_porter found on database");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        List<String> urls = searchText("Информационная безопасность");
        System.out.println(urls.toString());
    }

}
