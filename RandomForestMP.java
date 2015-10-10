import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.tree.RandomForest;


import java.util.HashMap;
import java.util.regex.Pattern;

public final class RandomForestMP {

    

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println(
                    "Usage: RandomForestMP <training_data> <test_data> <results>");
            System.exit(1);
        }
        String training_data_path = args[0];
        String test_data_path = args[1];
        String results_path = args[2];

        SparkConf sparkConf = new SparkConf().setAppName("RandomForestMP");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final RandomForestModel model;

        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        Integer numTrees = 3;
        String featureSubsetStrategy = "auto";
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        Integer seed = 12345;

		// TODO
        Pattern comma = Pattern.compile(",");
         
	   JavaRDD<LabeledPoint> training = sc.textFile(training_data_path).map(new Function<String, LabeledPoint>(){
            public LabeledPoint call(String line) throws Exception {
                String[] tokens = comma.split(line);
                double label = Double.parseDouble(tokens[tokens.length-1]);
                double[] points = new double[tokens.length-1];
                for (int tokenCount = 0; tokenCount  < tokens.length - 1; tokenCount++) {
                    point[tokenCount] = Double.parseDouble(tok[tokenCount]);
                }
                return new LabeledPoint(label, Vectors.dense(point));
            }
        });
        
        JavaRDD<Vector> testing = sc.textFile(test_data_path).map(new Function<String, Vector>(){
            public Vector call(String line) throws Exception {
                String[] tokens = comma.split(line);
                double[] point = new double[tokens.length-1];
                for (int tokenCount = 0; tokenCount  < tokenCount.length - 1; tokenCount++) {
                    point[i] = Double.parseDouble(tokens[i]);
                }
                return Vectors.dense(point);
            }
        });

	  /* model built using training data */ 
	  
	  RandomForestModel model = 
        org.apache.spark.mllib.tree.RandomForest.trainClassifier(
            training,
            numClasses, 
            categoricalFeaturesInfo,
            numTrees,
            featureSubsetStrategy,
            impurity,
            maxDepth,
            maxBins,
            seed
        );

	   /* test model using model built from training data */
        
        JavaRDD<LabeledPoint> results = testing.map(new Function<Vector, LabeledPoint>() {
            public LabeledPoint call(Vector points) {
                return new LabeledPoint(model.predict(points), points);
            }
        });

        results.saveAsTextFile(results_path);

        sc.stop();
    }

}
