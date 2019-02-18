package ru.kpfu.itis.group11501.popov.infosearch.parser;

import com.sun.deploy.util.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;


/**
 * Created by Bogdan Popov on 17.02.2019.
 */
public class ParseAndAddToDatabase {

    public static UUID studentId;

    public static void main(String[] args) throws IOException {
        String site = "https://xakep.ru/";
        Document doc = Jsoup.connect(site).get();
        Elements entryTitles = doc.select("h3.entry-title");
        List<String> linkStrings = entryTitles.select("a[href^=https://]").eachAttr("href").subList(0, 30);
        List<String> titles = entryTitles.select("a[href^=https://]").select("span").eachText().subList(0, 30);
        createStudent("Богдан", "Попов", "11-501");
        for (String link : linkStrings) {
            doc = Jsoup.connect(link).get();
            String title = titles.get(linkStrings.indexOf(link));
            String article = StringUtils.join(doc.select("div.bdaia-post-content").select("p").eachText(), " ");
            String tags = StringUtils.join(doc.select("div.tagcloud").select("a[rel=tag]").eachText(), ";");
            createArticle(title, tags, article, link, studentId);
        }
        String studentsTable = "students";
        String articlesTable = "articles";
        String xmlStudents = getXML(studentsTable);
        String xmlArticles = getXML(articlesTable);
        writeIntoTheFile(xmlStudents, studentsTable);
        writeIntoTheFile(xmlArticles, articlesTable);
        try {
            ConnectionSingleton.getConnection().close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createStudent(String name, String surname, String group) {
        try {
            Connection conn = ConnectionSingleton.getConnection();
            String query = " insert into students (id, name, surname, mygroup) values (?, ?, ?, ?)";
            PreparedStatement ps = conn.prepareStatement(query);
            studentId = UUID.randomUUID();
            ps.setObject(1, studentId);
            ps.setString(2, name);
            ps.setString(3, surname);
            ps.setString(4, group);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createArticle(String title, String keywords, String content,
                                     String url, UUID studentId) {
        try {
            Connection conn = ConnectionSingleton.getConnection();
            String query = " insert into articles (id, title, keywords, content, url, student_id) " +
                    "values (?, ?, ?, ?, ?, ?)";
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setObject(1, UUID.randomUUID());
            ps.setString(2, title);
            ps.setString(3, keywords);
            ps.setString(4, content);
            ps.setString(5, url);
            ps.setObject(6, studentId);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static String getXML(String table) {
        try {
            Connection conn = ConnectionSingleton.getConnection();
            String query = "SELECT table_to_xml(?, true, false, '')";
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setString(1, table);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getString(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void writeIntoTheFile(String xml, String filename) {
        try {
            File file = new File("D:\\repositories\\infosearch\\src\\main\\resources\\" + filename + ".xml");
            PrintWriter pw = new PrintWriter(file);
            pw.print(xml);
            pw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}
