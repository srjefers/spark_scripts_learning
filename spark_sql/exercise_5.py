from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from pyspark.sql import SparkSession

# Define the schema for the MovieRatings case class
MovieRatingsSchema = ArrayType([
    StructField("movieName", StringType(), nullable=False),
    StructField("rating", DoubleType(), nullable=False)
])

# Define the schema for the MovieCritics case class
MovieCriticsSchema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("movieRatings", ArrayType(MovieRatingsSchema), nullable=False)
])

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a list of MovieCritics
movies_critics = [
    (
        "Manuel",
        [
            ["movieName": "Logan", "rating": 1.5],
            ["movieName": "Zoolander", "rating": 3.0],
            ["movieName": "John Wick", "rating": 2.5]
        ]
    ),
    (
        "John",
        [
            ["movieName": "Logan", "rating": 2.0],
            ["movieName": "Zoolander", "rating": 3.5],
            ["movieName": "John Wick", "rating": 3.0]
        ]
    )
]

# Create a DataFrame from the list of MovieCritics
ratings = spark.createDataFrame(movies_critics, schema=MovieCriticsSchema)

ratings.show()