#                                        DAG


# 1ere Etape: 
from airflow import DAG
from airflow.operators.python import PythonOperator # Pour faire appel a un objet python (Ex.function python)
from datetime import datetime  # Pour les date d'execution de notre pipeline


# 2e Etape: Creer une instance de la classe DAG
with DAG('mon_dag', start_date=datetime(2023, 12, 31),
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