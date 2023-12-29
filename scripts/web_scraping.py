#!pip install openpyxl # Pour le moetur d'extraction de donnees a partir du site
import requests # Pour les requetes au server du site web
import pandas as pd # Pour le traitement de donnees (Ex:Enregistrer la donnee extraite sous format CSV)
from io import BytesIO # Pour les Entrees/Sorties en Octets
from mapper import mapper1



from pyspark.sql import SparkSession # Classe de SparkSession
from pyspark import SparkConf # Pour nos runtime configurations
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType # Nos types de donnees
import findspark # Trouver Spark dans notre systeme

findspark.init() # Demarrer findspark



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






#                                               WEB SCRAPING


# L'URL des donnees que nous voulons scraper 
excel_url1 = 'https://static.data.gouv.fr/resources/election-presidentielle-des-10-et-24-avril-2022-resultats-definitifs-du-1er-tour/20220414-152612/resultats-par-niveau-burvot-t1-france-entiere.xlsx'



# Definissons une fonction qui va recevoir un URl, et aller extraire(scraper) des donnees a partir de l'URL
def download_excel(url):
    response = requests.get(url) # Lancer une requête au serveur https
    if response.status_code == 200: # Si la requête est acceptée
        return response.content # On returne le contenu de la requête
    else: # Au cas contaire, on affiche la raison du rejet de la requête
        raise Exception(f"Echec du téléchargement du fichier Excel. Etat du code: {response.status_code}")

# La function principale
def main():
    try: # Essaie
        # Un appel a la function download_excel definie en haut
        excel_content = download_excel(excel_url)

        # On va maintenant convertir le contenu en un  DataFrame de pandas en utilisant le moteur 'openpyxl'
        df = pd.read_excel(BytesIO(excel_content), engine='openpyxl')

        # Apres, l'extraction a partir du site web, on va maintenant sauvegarder le contenu sous format CSV
        df.rename(columns=mapper1, inplace=True) # Renommer les columns en function des regles de MySQl (Ex. Enlver les accents et espaces)
        

        #                           SPARK DATAFRAME

        spark_df = spark.createDataFrame(df) # Creer un dataframe spark a partir de notre pandas dataframe
        print(spark_df.show())


        #print(df.head(3)) # Afficher les 3 permieres lignes de notre pandas dataframe


        #                            ENREGISTRER LES DONNEES SUR LE DISQUE DUR LOCAL (SI ON LE SOUHAITE)
        #df.to_csv('C:/Users/maham/OneDrive/Documents/pipeline/data.csv', index=False)


    except Exception as e: # S'il ya erreur
        print(f"Error: {e}") # Alors affiche l'erreur



# Faire appel a la fonction principale
if __name__ == "__main__":
    main() # Lancement du programme principal

