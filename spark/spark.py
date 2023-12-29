#!pip install openpyxl # Pour le moetur d'extraction de donnees a partir du site
import requests # Pour les requetes au server du site web
import pandas as pd # Pour le traitement de donnees (Ex:Enregistrer la donnee extraite sous format CSV)
from io import BytesIO # Pour les Entrees/Sorties en Octets
from mapper import mapper1 # Pour changer les noms des colonnes de notre pandas dataframe
from struct_type import schema # Pour changer les noms des colonnes de notre spark dataframe


import pandas as pd 

# Pour la visualisation
import matplotlib.pyplot as plt
import seaborn as sns
plt.style.use('fivethirtyeight')


from pyspark.sql import SparkSession # Classe de SparkSession
from pyspark import SparkConf # Pour nos runtime configurations
from pyspark.sql.functions import *  # Importer toutes les functions dans pyspark.sql.functions
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType # Nos types de donnees

import findspark # Trouver Spark dans notre systeme

findspark.init() # Demarrer findspark


#                                   SPARK SESSION

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




# La function de web scraping
def web_scraping():
    try: # Essaie
        # Un appel a la function download_excel definie en haut
        excel_content = download_excel(excel_url)

        # On va maintenant convertir le contenu en un  DataFrame de pandas en utilisant le moteur 'openpyxl'
        pandas_df = pd.read_excel(BytesIO(excel_content), engine='openpyxl')

        # Apres, l'extraction a partir du site web, on va maintenant sauvegarder le contenu sous format CSV
        pandas_df.rename(columns=mapper1, inplace=True) # Renommer les columns en function des regles de MySQl (Ex. Enlver les accents et espaces)
        #print(pandas_df.head(3)) # Afficher les 3 permieres lignes de notre pandas dataframe


        #       ENREGISTRER LES DONNEES SUR LE DISQUE DUR LOCAL (SI ON LE SOUHAITE)
        #pandas_df.to_csv('C:/Users/maham/OneDrive/Documents/pipeline/data.csv', index=False)

    except Exception as e: # S'il ya erreur
        print(f"Error: {e}") # Alors affiche l'erreur


    return pandas_df



# Creer un objet: Pandas DataFrame
pandas_df = web_scraping()
  



# Une fonction pour convertir un pandas dataframe en un spark dataframe 
def creer_sparkDf_apartir_pandasDf(pandas_df):
    df = spark.createDataFrame(pandas_df, schema=schema) # Creer un dataframe spark a partir de notre pandas dataframe
    return df



# Creer un objet: Spark DataFrame
df = creer_sparkDf_apartir_pandasDf(pandas_df)



# Une fonction pour afficher les 20 premieres lignes de notre spark dataframe
def afficher_head_df(df):
    print(df.show())       



# Une fonction pour verifier les types de donnees de notre spark dataframe
def verifier_types_donees(df):
    print(df.printSchema())



# Une fonction pour afficher le nombre de lignes et de colonnes de notre spark dataframe
def afficher_shape(df):
    print((df.count(), len(df.columns)))



# Une fonction pour verifier les valeurs manquantes dans notre spark dataframe
def verifier_valeurs_manquantes(df):
    print(df.select([count(when(isnan(c), c)).alias(c) for c in df.columns]).show())



# Une fonction pour faire le graphe des resultas d'un departement donne
def resulats_d_un_dep_donne(departement : str, dataframe):    

    m = {
    'voix_ARTHAUD_Nathalie' : 'Nathalie',
    'voix_ROUSSEL_Fabien' : 'ROUSSEL',
    'voix_MACRON_Emmanuel' : 'MACRON',
    'voix_LASSALLE_Jean' : 'LASSALLE',
    'voix_LE_PEN_Marine' : 'LE PEN',
    'voix_ZEMMOUR_Eric' : 'ZEMMOUR',
    'voix_MELENCHON_Jean_Luc' : 'MELENCHON',
    'voix_HIDALGO_Anne' : 'Anne',
    'voix_JADOT_Yannick' : 'Yannick',
    'voix_PECRESSE_Valerie' : 'Valerie',
    'voix_POUTOU_Philippe' : 'Philippe',
    'voix_DUPONT_AIGNAN_Nicolas' : 'Nicolas'
    }


    print(f'Les resutats finaux des voix dans le departement de {departement} par ordre decroissant sont: ')
    dict_voix_dep = {} # Un dictionnaire contenant toutes les voix par candidat par departement
    for col in dataframe.columns: # Les noms des candidats
        
        dict_voix_dep[col] = dataframe.agg(sum(col)).collect()[0][0] # Ajouter le nouveau candidta et ses resultas au dictionnaire
        
    list_voix_dep = sorted(dict_voix_dep.items(), key=lambda x: x[1], reverse=True) # Une liste de tuple (candidat, voix) par ordre decroissant
    
    d = pd.DataFrame(data=list_voix_dep, columns=['candidat', 'voix']) # Creer un pandas datframe des resulats par candidat et par departement
    
    x = [list_voix_dep[0][0],  list_voix_dep[1][0],list_voix_dep[2][0],list_voix_dep[3][0],list_voix_dep[4][0],list_voix_dep[5][0],
    list_voix_dep[6][0],list_voix_dep[7][0],list_voix_dep[8][0],list_voix_dep[9][0],list_voix_dep[10][0],list_voix_dep[11][0]] # Les valuer de l'axe des Xs
    
    sns.barplot(data=d, x= x, y='voix') # Un barplot des resutats par ordre decroissant
    plt.show();


departements = ['Ain', 'Aisne', 'Allier', 'Alpes-de-Haute-Provence', 'Hautes-Alpes', 'Alpes-Maritimes', 'Ardèche', 'Ardennes', 'Ariège','Aube', 'Aude', 'Aveyron', 
                'Bouches-du-Rhône', 'Calvados', 'Cantal', 'Charente', 'Charente-Maritime', 'Cher', 'Corrèze', 'Corse-du-Sud', 'Haute-Corse', "Côte-d'Or", 
                "Côtes-d'Armor", 'Creuse', 'Dordogne', 'Doubs', 'Drôme', 'Eure', 'Eure-et-Loir', 'Finistère', 'Gard', 'Haute-Garonne', 'Gers', 'Gironde', 'Hérault',
                'Ille-et-Vilaine', 'Indre', 'Indre-et-Loire', 'Isère', 'Jura', 'Landes', 'Loir-et-Cher', 'Loire', 'Haute-Loire','Loire-Atlantique', 'Loiret', 
                'Lot', 'Lot-et-Garonne', 'Lozère', 'Maine-et-Loire', 'Manche', 'Marne', 'Haute-Marne', 'Mayenne', 'Meurthe-et-Moselle', 'Meuse', 'Morbihan', 
                'Moselle', 'Nièvre', 'Nord', 'Oise', 'Orne', 'Pas-de-Calais', 'Puy-de-Dôme', 'Pyrénées-Atlantiques', 'Hautes-Pyrénées', 'Pyrénées-Orientales',
                'Bas-Rhin', 'Haut-Rhin', 'Rhône', 'Haute-Saône', 'Saône-et-Loire', 'Sarthe', 'Savoie', 'Haute-Savoie', 'Paris', 'Seine-Maritime', 'Seine-et-Marne', 
                'Yvelines', 'Deux-Sèvres', 'Somme', 'Tarn', 'Tarn-et-Garonne', 'Var', 'Vaucluse', 'Vendée', 'Vienne','Haute-Vienne', 'Vosges', 'Yonne', 
                'Territoire de Belfort', 'Essonne', 'Hauts-de-Seine', 'Seine-Saint-Denis', 'Val-de-Marne', "Val-d'Oise", 'Guadeloupe', 'Martinique', 
                'Guyane', 'La Réunion', 'Mayotte', 'Nouvelle-Calédonie', 'Polynésie française', 'Saint-Pierre-et-Miquelon', 'Wallis et Futuna', 
                'Saint-Martin/Saint-Barthélemy', 'Français établis hors de France']



# Une fonction pour faire tous les graphes de tous les 107 departements de notre spark dataframe
def resulats_des_departements():
    for departement in departements:
        print('*' * 140)
        print('\n\n')
        resulats_d_un_dep_donne(departement=departement, dataframe=df.filter(df.libelle_departement == departement).select(df.colRegex("`^voix.*`")).withColumnsRenamed(m))
        print('\n\n')





# Une fonction pour faire le graphe des resultas d'une commune donnee
def resulats_d_une_commune_donnee(commune : str, dataframe):    

    m = {
    'voix_ARTHAUD_Nathalie' : 'Nathalie',
    'voix_ROUSSEL_Fabien' : 'ROUSSEL',
    'voix_MACRON_Emmanuel' : 'MACRON',
    'voix_LASSALLE_Jean' : 'LASSALLE',
    'voix_LE_PEN_Marine' : 'LE PEN',
    'voix_ZEMMOUR_Eric' : 'ZEMMOUR',
    'voix_MELENCHON_Jean_Luc' : 'MELENCHON',
    'voix_HIDALGO_Anne' : 'Anne',
    'voix_JADOT_Yannick' : 'Yannick',
    'voix_PECRESSE_Valerie' : 'Valerie',
    'voix_POUTOU_Philippe' : 'Philippe',
    'voix_DUPONT_AIGNAN_Nicolas' : 'Nicolas'
    }


    print(f'Les resutats finaux des voix dans la commune de {commune} par ordre decroissant sont: ')
    dict_voix_commune = {} # Un dictionnaire contenant toutes les voix par candidat par commune
    for col in dataframe.columns: # Les noms des candidats
        
        dict_voix_commune[col] = dataframe.agg(sum(col)).collect()[0][0] # Ajouter le nouveau candidta et ses resultas au dictionnaire
        
    list_voix_commune = sorted(dict_voix_commune.items(), key=lambda x: x[1], reverse=True) # Une liste de tuple (candidat, voix) par ordre decroissant
    
    d = pd.DataFrame(data=list_voix_commune, columns=['candidat', 'voix']) # Creer un pandas datframe des resulats par candidat et par commune
    
    x = [list_voix_commune[0][0],  list_voix_commune[1][0],list_voix_commune[2][0],list_voix_commune[3][0],list_voix_commune[4][0],list_voix_commune[5][0],
    list_voix_commune[6][0],list_voix_commune[7][0],list_voix_commune[8][0],list_voix_commune[9][0],list_voix_commune[10][0],list_voix_commune[11][0]] # Les valuer de l'axe des Xs
    
    sns.barplot(data=d, x= x, y='voix') # Un barplot des resutats par ordre decroissant
    plt.show();



# Puisqu'il ya 32988 ommunes dans nos donnees, nous allons choisir au hasard, 20 communes 
communes = ['Ceyzérieu', 'Limé', 'Saint-Algis', 'Vichel-Nanteuil', 'Sazeret', 'Le Castellard-Melan', 'Châteauneuf-Miravail', 'Ceillac', 'Gréolières', 'Bogy', 
            'Châteaubourg', 'Etables', 'Payzac', 'Saint-Jean-le-Centenier', 'Vals-les-Bains', 'Chesnois-Auboncourt  ', 'Quilly ', 
            'Saint-Lambert-et-Mont-de-Jeux', 'Sommauthe', 'Barjac']


# Une fonction pour faire tous les graphes des 20 communes choisies par hasard
def resulats_des_communes():
    for commune in communes:
        print('*' * 140)
        print('\n\n')
        resulats_d_une_commune_donnee(commune=commune, dataframe=df.filter(df.libelle_commune == commune).select(df.colRegex("`^voix.*`")).withColumnsRenamed(m))
        print('\n\n')







#                                        DAG


# 1ere Etape: 
from airflow import DAG
from airflow.operators.python import PythonOperator # Pour faire appel a un objet python (Ex.function python)
from datetime import datetime  # Pour les date d'execution de notre pipeline


# 2e Etape: Creer une instance de la classe DAG
with DAG('myDag', start_date=datetime(2023, 12, 31),
         schedule_interval='@daily', catchup=False) as dag:  # Gestionnaire de context 

    # 1ere tache
    createSparkRuntimeConfigurations_Task = PythonOperator(task_id='createSparkRuntimeConfigurations_TaskId',python_callable=createSparkRuntimeConfigurations)
   
    # 2eme tache
    createSparkSession_Task = PythonOperator(task_id='createSparkSession_TaskId',python_callable=createSparkSession)

    # 3eme tache
    web_scraping_Task = PythonOperator(task_id='web_scraping_TaskId',python_callable=web_scraping)

    # 4eme tache
    creer_sparkDf_apartir_pandasDf_Task = PythonOperator(task_id='creer_sparkDf_apartir_pandasDf_TaskId',python_callable=creer_sparkDf_apartir_pandasDf)

    # 5eme tache
    afficher_head_df_Task = PythonOperator(task_id='afficher_head_df_TaskId',python_callable=afficher_head_df)
    
    # 6eme tache
    verifier_types_donees_Task = PythonOperator(task_id='verifier_types_donees_TaskId',python_callable=verifier_types_donees)
    
    # 7eme tache
    afficher_shape_Task = PythonOperator(task_id='afficher_shape_TaskId',python_callable=afficher_shape)
    
    # 8eme tache
    verifier_valeurs_manquantes_Task = PythonOperator(task_id='verifier_valeurs_manquantes_TaskId',python_callable=verifier_valeurs_manquantes)
    
    # 9eme tache
    resulats_des_departements_Task = PythonOperator(task_id='resulats_des_departements_TaskId',python_callable=resulats_des_departements)
    
    # 10eme tache
    resulats_des_communes_Task = PythonOperator(task_id='resulats_des_communes_TaskId',python_callable=resulats_des_communes)
    
   

    # Les dependences entre les taches
    createSparkRuntimeConfigurations_Task >> createSparkSession_Task >> web_scraping_Task
    web_scraping_Task >> creer_sparkDf_apartir_pandasDf_Task
    creer_sparkDf_apartir_pandasDf_Task >> afficher_head_df_Task >> verifier_types_donees_Task
    verifier_types_donees_Task >> [afficher_shape_Task, verifier_valeurs_manquantes_Task]
    [afficher_shape_Task, verifier_valeurs_manquantes_Task] >> resulats_des_departements_Task
    resulats_des_departements_Task >> resulats_des_communes_Task