
import java.sql.*;

public class SalesAnalysis {
    public static void main(String[] args) {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://localhost:10000/default";
        String username = "";
        String password = "";
        try {
            Class.forName(driverName);
            Connection conn = DriverManager.getConnection(url, username, password);
            Statement stmt = conn.createStatement();
            // Thực hiện truy vấn Hive để lấy tổng doanh số bán hàng trong mỗi ngày/tháng/năm
            String query = "SELECT SUBSTR(timestamp, 1, 10) AS sale_date, " +
                           "SUM(quantity * unit_price) AS total_sales " +
                           "FROM transactions " +
                           "GROUP BY SUBSTR(timestamp, 1, 10)";
            ResultSet rs = stmt.executeQuery(query);
            // In ra kết quả
            System.out.println("Doanh số bán hàng theo ngày:");
            while (rs.next()) {
                String saleDate = rs.getString("sale_date");
                double totalSales = rs.getDouble("total_sales");
                System.out.println(saleDate + ": " + totalSales);
            }
            rs.close();
            stmt.close();
            conn.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
   // Thực hiện truy vấn Hive để đếm số lượng giao dịch trong mỗi ngày/tháng/năm
            String query = "SELECT SUBSTR(timestamp, 1, 10) AS transaction_date, " +
                           "COUNT(*) AS transaction_count " +
                           "FROM transactions " +
                           "GROUP BY SUBSTR(timestamp, 1, 10)";
            ResultSet rs = stmt.executeQuery(query);
            // In ra kết quả
            System.out.println("Số lượng giao dịch theo ngày:");
            while (rs.next()) {
                String transactionDate = rs.getString("transaction_date");
                int transactionCount = rs.getInt("transaction_count");
                System.out.println(transactionDate + ": " + transactionCount);
            }
  // Thực hiện truy vấn Hive để đếm số lượng giao dịch cho mỗi sản phẩm
            String query = "SELECT product_id, COUNT(*) AS transaction_count " +
                           "FROM transactions " +
                           "GROUP BY product_id " +
                           "ORDER BY transaction_count DESC " +
                           "LIMIT 10";
            ResultSet rs = stmt.executeQuery(query);
            // In ra top 10 sản phẩm bán chạy nhất
            System.out.println("Top 10 sản phẩm bán chạy nhất:");
            while (rs.next()) {
                int productId = rs.getInt("product_id");
                int transactionCount = rs.getInt("transaction_count");
                System.out.println("Product ID: " + productId + ", Số lượng giao dịch: " + transactionCount);
            }
              // Thực hiện truy vấn Hive để tính tổng chi tiêu của mỗi khách hàng
            String query = "SELECT customer_id, SUM(quantity * unit_price) AS total_spent " +
                           "FROM transactions " +
                           "GROUP BY customer_id " +
                           "ORDER BY total_spent DESC " +
                           "LIMIT 10";
            ResultSet rs = stmt.executeQuery(query);
            // In ra top 10 khách hàng có chi tiêu cao nhất
            System.out.println("Top 10 khách hàng có chi tiêu cao nhất:");
            while (rs.next()) {
                int customerId = rs.getInt("customer_id");
                double totalSpent = rs.getDouble("total_spent");
                System.out.println("Customer ID: " + customerId + ", Tổng chi tiêu: " + totalSpent);
            }
 // Thực hiện truy vấn Hive để lấy ra sản phẩm có doanh số cao nhất trong mỗi tháng
            String query = "SELECT month(timestamp) AS month, product_id, SUM(quantity * unit_price) AS total_sales " +
                           "FROM transactions " +
                           "GROUP BY month(timestamp), product_id";
            ResultSet rs = stmt.executeQuery(query);
            // Tạo một bảng tạm thời để lưu trữ sản phẩm có doanh số cao nhất trong mỗi tháng
            int[] maxProductId = new int[12]; // 12 tháng trong năm
            double[] maxTotalSales = new double[12];
            // Khởi tạo giá trị ban đầu cho bảng tạm thời
            for (int i = 0; i < 12; i++) {
                maxProductId[i] = -1; // Không có sản phẩm nào được chọn ban đầu
                maxTotalSales[i] = 0.0; // Doanh số ban đầu là 0
            }
            // Lặp qua kết quả truy vấn để chọn sản phẩm có doanh số cao nhất trong mỗi tháng
            while (rs.next()) {
                int month = rs.getInt("month") - 1; // Chỉ số mảng bắt đầu từ 0
                int productId = rs.getInt("product_id");
                double totalSales = rs.getDouble("total_sales");
                if (totalSales > maxTotalSales[month]) {
                    maxProductId[month] = productId;
                    maxTotalSales[month] = totalSales;
                }
            }
            // In ra sản phẩm có doanh số cao nhất trong mỗi tháng
            for (int i = 0; i < 12; i++) {
                if (maxProductId[i] != -1) {
                    System.out.println("Tháng " + (i + 1) + ": Sản phẩm " + maxProductId[i] + ", Doanh số: " + maxTotalSales[i]);
                }
            }
        
    }
}
