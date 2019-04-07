package ru.kpfu.itis.group11501.popov.infosearch.parser;

import Jama.Matrix;
import Jama.SingularValueDecomposition;
import javafx.util.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static ru.kpfu.itis.group11501.popov.infosearch.parser.CosSearch.*;
import static ru.kpfu.itis.group11501.popov.infosearch.parser.LemmatizeAndAddToDatabase.findAll;
import static ru.kpfu.itis.group11501.popov.infosearch.parser.LemmatizeAndAddToDatabase.getWordsFromText;

public class LSA {

    // Singular decomposition value
    public static final int K = 5;

    public static List<String> getTerms() {
        ResultSet rs = findAll("terms_list");
        List<String> terms = new ArrayList<>();
        try {
            if (rs != null) {
                while (rs.next()) {
                    terms.add(rs.getString("term"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return terms;
    }

    public static List<Pair<String, Double>> searchText(String searchText) {
        List<Pair<String, Double>> result = new ArrayList<>();
        List<String> requestWords = getWordsFromText(searchText);
        requestWords = requestWords.stream().map(LemmatizeAndAddToDatabase::lemmatiseWithPorter).collect(Collectors.toList());
        List<Map<String, Object>> articlesByTerms = findArticlesByTerms(requestWords);
        if (articlesByTerms != null) {
            List<String> terms = getTerms();
            Map<String, List<Double>> map = getArticleVectors(terms, articlesByTerms);
            String [] articles = map.keySet().toArray(new String[0]);
            double [][] arr = new double[terms.size()][articles.length];
            for (int i = 0; i < terms.size(); i++) {
                for (int j = 0; j < articles.length; j++) {
                    arr[i][j] = Optional.ofNullable(map.get(articles[j]).get(i)).orElse(0.0);
                }
            }
            Matrix matrix = new Matrix(arr);
            SingularValueDecomposition decomposition = matrix.svd();
            Matrix S = decomposition.getS();
            Matrix U = decomposition.getU();
            Matrix V = decomposition.getV();
            //reduce dimension
            Matrix Sk = S.getMatrix(0, K - 1, 0, K - 1);
            Matrix Uk = U.getMatrix(0, U.getRowDimension() - 1, 0, K - 1);
            Matrix Vk = V.getMatrix(0, V.getRowDimension() - 1, 0, K - 1);

            double[][] arrVk = Vk.getArray();

            double[][] queryArr = new double[1][terms.size()];
            for (int i = 0; i < terms.size(); i++) {
                if (requestWords.contains(terms.get(i))) {
                    queryArr[0][i] = 1;
                } else {
                    queryArr[0][i] = 0;
                }
            }
            Matrix queryMatrix = new Matrix(queryArr);
            Matrix inverseSk = Sk.inverse();
            Matrix svdQueryMatrix = queryMatrix.times(Uk).times(inverseSk);
            List<Double> queryVector = Arrays.stream(svdQueryMatrix.getArray()[0]).boxed().collect(Collectors.toList());
            for (int i = 0; i < articles.length; i++) {
                List<Double> articleVector = Arrays.stream(arrVk[i]).boxed().collect(Collectors.toList());
                result.add(new Pair<>(articles[i], cosMeasure(queryVector, articleVector)));
            }
            result.sort(Comparator.comparing(Pair<String, Double>::getValue).reversed());
            result = result.subList(0, 10);
        }
        return result;
    }

    public static void main(String[] args) {
        List<Pair<String, Double>> result = searchText(
                        "Для эксплуатации бага злоумышленник должен был заманить жертву " +
                        "на свой вредоносный сайт, где специальный код JavaScript «прощупывал» " +
                        "определенные URL, связанные с учетной записью пользователя в Google Photos. " +
                        "Скрипт полагается на время и размер полученных ответов (даже если это ответ «" +
                        "доступ запрещен»), дабы выявить определенные артефакты учетной записи."
        );
        for (Pair<String, Double> resultEntry : result) {
            System.out.println("URL: " + resultEntry.getKey() + " cos: " + resultEntry.getValue());
        }
    }
}
