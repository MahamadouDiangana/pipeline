# _`Projet : Prédiction du Vote aux Présidentielles Françaises 2022`_ <br>

_`Ce projet analyse les données électorales des différentes localités en France pour prédire le vote d’une personne en fonction de sa localisation.`_

## _`Objectif`_
***"Dis-moi où tu vis, je te dirai pour qui tu vas voter."***
_`En intégrant et analysant les données électorales régionales, ce projet vise à établir une corrélation entre la localisation géographique et les préférences de vote pour les élections présidentielles de 2022.`_

## _`Technologies Utilisées`_
<b><h2><i>   Apache NiFi :</i></h2></b> _`Pour l’ingestion de données, facilitant l’extraction et la transformation des données électorales.`_ <br> 
<b><h2><i> Apache Kafka : </i></h2></b>  _`Pour le streaming des données vers des topics dédiés, assurant une communication efficace entre les composantes.`_ <br>
<b><h2><i>   Apache Spark : </i></h2></b> _`Pour le calcul distribué des Big Data, permettant une analyse rapide et scalable des données électorales. `_<br>
<b><h2><i> Docker et Docker Compose : </i></h2></b>  _`Pour la conteneurisation de chaque composant du projet, assurant portabilité et simplification de l’environnement de développement.`_ <br><br>

## _`Fonctionnalités Clés`_
<b><h2><i> Ingestion et Transformation : </i></h2></b>  _`Utilisation d’Apache NiFi pour automatiser l’ingestion des données brutes et assurer leur traitement en temps réel.`_ <br>
<b><h2><i> Streaming des Données : </i></h2></b>  _`Grâce à Apache Kafka, les données sont diffusées en continu vers des topics dédiés. `_ <br>
<b><h2><i> Analyse Distribuée : </i></h2></b>  _`Apache Spark traite les données à grande échelle, en utilisant des techniques de calcul distribué pour gérer les Big Data. `_ <br>
<b><h2><i> Conteneurisation : </i></h2></b>  _`Docker et Docker Compose permettent de déployer facilement chaque composant, facilitant le déploiement et la reproductibilité du projet. `_ <br><br>

## _`Structure du Projet`_
<b><h2><i> nifi/ : </i></h2></b>  _`Contient les templates et configurations pour le flux de travail d’Apache NiFi. `_ <br>
<b><h2><i> kafka/ : </i></h2></b>  _`Configuration des topics Kafka pour le streaming des données électorales. `_ <br>
<b><h2><i> spark/ : </i></h2></b>  _`Scripts Spark pour le calcul distribué et l’analyse des données. `_ <br>
<b><h2><i> docker-compose.yml : </i></h2></b>  _`Fichier de configuration pour orchestrer les différents conteneurs via Docker Compose. `_ <br>
