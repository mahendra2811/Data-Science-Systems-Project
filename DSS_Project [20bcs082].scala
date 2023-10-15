import org.apache.spark.graphx.{Graph, GraphLoader}

object ConnectedComponentsAnalysis {
    def main(args: Array[String]): Unit = {
	// Create a SparkSession for our Connected Components Analysis
        val spark = SparkSession.builder()
        .appName("Connected Components Analysis")
        .getOrCreate()

	// Load the graph from the specified dataset file
        val graph = GraphLoader.edgeListFile(spark.sparkContext, "D:/7th Sem/Data Science System/Project/Amazon0601.txt")

	// Determine the connected components in the graph
        val connectedComponents = graph.connectedComponents().vertices

	// Calculate the size of each connected component
        val componentSizes = connectedComponents
        .map { case (_, componentId) => (componentId, 1) }
        .reduceByKey(_ + _)

	// Sort the connected components by size in descending order
        val sortedComponents = componentSizes
        .sortBy { case (_, size) => size }
        .collect()
        .reverse

	// Display information about the connected components
        for ((componentId, size) <- sortedComponents) {
            println(s"Connected Component ID: $componentId, Component Size: $size")
        }

	// Stop the SparkSession to release resources
        spark.stop()
    }
}