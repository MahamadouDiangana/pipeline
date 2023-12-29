from struct_type import schema # Importer la variable schema a partir du fichier struct_type.py
from confluent_kafka import Consumer, KafkaError # Des API en python pour consommer notre topic Kafka


from pyspark.sql import SparkSession # Classe de SparkSession
from pyspark import SparkConf # Pour nos runtime configurations
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType # Nos types de donnees
import findspark # Trouver Spark dans notre systeme

findspark.init() # Demarrer findspark




# Configurons notre consommateur Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Nos brokers Kafka  
    'group.id': 'test-consumer-group',  #  Nous allons consommer les donnees en tant que groupe: "test-consumer-group"
    'auto.offset.reset': 'earliest',  # Nous voulons cosommer les donnees a partir du debut 
}


# Creons notre consommateur Kafka
consumer = Consumer(conf)

# Nous allons nous abonner a notre topic Kafka 
topic = 'data'  # Le nom de notre topic
consumer.subscribe([topic])

# Une liste vide dans laquelle nous allons mettre toutes les donnees consommees a partir de notre topic kafka
messages = [] 

try:
    while True:
        # Lire les donnees a partir de notre topic
        msg = consumer.poll(timeout=1000)  

        if msg is None: # Si notre message est vide
            continue
        if msg.error(): # S'il y a une erreur
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Qund on arrive a la fin des donnees dans le topic, 
                print("Arriver a la fin partition") # Affiche le message suivant ("Arriver a la fin partition")
            else:
                print(f"Erreur: {msg.error()}") # Affiche l'erreur
        else: # Si tout se passe bien
            # Traite et decode le message reçu
            #print(f"Message reçu: {msg.value().decode('utf-8')}")
            messages.append((msg.value().decode('utf-8'),))

except KeyboardInterrupt:
    # Si nous arretons par erreur le programme pendent la consommation, n'arrete pas la consommation
    pass
finally:
    # Arrete le consummmateur quand toute la donnee a ete consommee a partir du topic
    consumer.close()




# Definir une finction qui definit nos runtime configurations
def createSparkRuntimeConfigurations():
    # scala_version = '2.12'
    # spark_version = '3.5.0'
    # kafka_client_version = '3.6.1'
    conf = SparkConf()\
           .setAppName("Pipeline")\
           .setMaster("local[*]")\
           .set("spark.executor.memory", "3g")\
           .set("spark.driver.maxResultSize", "3g")\
           .set("park.executor.heartbeatInterval","3600s") \
           .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
           .set("spark.hadoop.home.dir", "C:\\hadoop") 
    
    return conf

# Definir une finction qui prend en parametre nos runtime cinfigurations et cree une SparSession avec
def createSparkSession(conf):
    spark = SparkSession \
           .builder \
           .config(conf=conf) \
           .getOrCreate()
           
    return spark

# Creer une instance de la classe SparkConf pour nos runtime configurations
conf = createSparkRuntimeConfigurations()
# Demarrer notre Session de Spark
spark = createSparkSession(conf)


# Convertir la liste qui contient nos messages(donnees) Kafka en un Spark DataFrame
df = spark.createDataFrame(messages, schema=schema)

# Montrer les 20 permieres lignes de notre Spark DataFrame
print(df.show())

# Arreter notre Spark session
#spark.stop()