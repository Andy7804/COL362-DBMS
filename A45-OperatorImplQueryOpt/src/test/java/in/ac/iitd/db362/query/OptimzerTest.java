package in.ac.iitd.db362.query;

import in.ac.iitd.db362.api.PlanBuilder;
import in.ac.iitd.db362.catalog.Catalog;
import in.ac.iitd.db362.executor.QueryExecutor;
import in.ac.iitd.db362.operators.Operator;
import in.ac.iitd.db362.optimizer.BasicOptimizer;
import in.ac.iitd.db362.optimizer.Optimizer;
import in.ac.iitd.db362.storage.DataLoader;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class OptimzerTest {

    @Test
    public void test1() throws Exception {

        //Create statistics
        Catalog catalog = new Catalog();
        DataLoader.createStatistics("data/csvTables/customer.csv", catalog);
        DataLoader.createStatistics("data/csvTables/orders.csv", catalog);
        DataLoader.createStatistics("data/csvTables/product.csv", catalog);


        String outputFile = "data/output/test_customer_orders_product_join_with_price_filter_output.csv";

        // Build the plan:
        // 1. Scan Customer.csv and filter customers with age > 30.
        // 2. Join with Orders.csv on customer_id.
        // 3. Join with Product.csv on product_id.
        // 4. Apply an additional filter: only keep rows where price > 20.
        // 5. Project the customer's name and the product name.
        // 6. Sink the result to an output CSV file.
        Operator plan = PlanBuilder.scan("data/csvTables/customer.csv")
                .join(PlanBuilder.scan("data/csvTables/orders.csv"), "c_customer_id = o_customer_id")
                .join(PlanBuilder.scan("data/csvTables/product.csv"), "o_product_id = p_product_id")
                .filter("p_price > 20")
                .filter("c_age > 30")
                .project("c_name", "p_product_name")
                .sink(outputFile)
                .build();

        // Execute the plan.
        QueryExecutor.execute(plan);


        // Initialize the optimizer ando optimize
        Optimizer optimizer = new BasicOptimizer(catalog);
        Operator optimizedPlan = optimizer.optimize(plan);

        // Execute the optimized plan
        QueryExecutor.execute(optimizedPlan);

    }

}
