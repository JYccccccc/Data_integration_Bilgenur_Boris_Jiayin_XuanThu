# Data_integration_Bilgenur_Boris_Jiayin_XuanThu
 
#Monitoring of Acidied Surface Waters 
 
Author :  Bilgenur OZDEMIR
			    Xuan Thu NGUYEN
          Boris Yves NZEUYO DJOKO  
          Jiayin CHEN 
Tuteur :  Lionel Brice SOUOP PEKAM  

#Introduction
La surveillance de l'acidification des eaux de surface est un enjeu environnemental crucial. Elle permet d'évaluer l'impact des dépôts atmosphériques d'azote et de soufre sur les écosystèmes aquatiques. Ce projet s'intéresse au développement d'un système d'intégration de données robuste capable de gérer des sources hétérogènes et d'analyser les tendances de la chimie des eaux de surface.
Objectifs
Ce rapport présente les travaux réalisés pour :
Comprendre la structure des ensembles de données disponibles (LTM_Data_2022_8_1, Methods_2022_8_1, Site_Information_2022_8_1) stockés dans HDFS.
Développer une application Kafka pour lire le fichier LTM_Data en continu et l'envoyer par paquets de 10 éléments vers Kafka, avec un intervalle de 10 secondes entre chaque envoi.
Intégrer les données en streaming provenant de Kafka avec les données statiques stockées dans HDFS afin d'enrichir la base de données.
Définir, calculer et stocker des indicateurs pertinents pour analyser les tendances de la chimie de l'eau.
Choisir une architecture de stockage adaptée aux besoins du projet.
Technologies
Le projet s'appuie sur les technologies suivantes :
Spark
Spark Streaming
Kafka
Kafka Connect (optionnel)

#Strucdure de projet
├── database
│   ├── LTM_Data_2022_8_1.xlsx
│   ├── Methods_2022_8_1.xlsx
│   └── Site_Information_2022_8_1.xlsx
├── hdfs_reader
│   ├── __pycache__
│   │   └── read_hdfs.cpython-312.pyc
│   └── read_hdfs.py
├── kafka
│   ├── __pycache__
│   │   └── consumer.cpython-312.pyc
│   └── producer.py
├── metrics_calculator
│   ├── __pycache__
│   │   └── calculate_metrics.cpython-312.pyc
│   └── calculate_metrics.py
├── show_resultat
│   ├── display_Methode.py
│   ├── display_data_integration.py
│   └── show.py
└── spark_streaming
    ├── integration.py
    └── stream_processor.py

Dans ce projet, nous avons une structure de répertoire détaillée avec différentes fonctionnalités :

Dossier database :
Ce dossier contient trois fichiers Excel principaux : LTM_Data_2022_8_1.xlsx, Methods_2022_8_1.xlsx, et Site_Information_2022_8_1.xlsx. Ces fichiers servent à stocker les données brutes sur la qualité de l'eau, les informations des sites d'échantillonnage et les méthodes de mesure des différents paramètres.

Dossier hdfs_reader :
Contient le script read_hdfs.py, qui sert à lire des données à partir du système de fichiers distribué Hadoop (HDFS). Le fichier read_hdfs.cpython-312.pyc dans le dossier __pycache__ est un fichier de cache pour ce script.

Dossier kafka :
Contient le script producer.py, utilisé pour écrire des données dans un topic Kafka.

Dossier metrics_calculator :
Contient le script calculate_metrics.py, qui calcule divers indicateurs et statistiques sur la qualité de l'eau.

Dossier show_resultat :
Contient trois scripts : display_Methode.py pour afficher les informations sur les méthodes de mesure, display_data_integration.py pour montrer les résultats de l'intégration des données, et show.py pour afficher les résultats globaux.

Dossier spark_streaming :
Contient les scripts integration.py et stream_processor.py, qui traitent et intègrent les données en temps réel à l'aide de Spark Streaming.

#Compréhension des données 
On a utilisé 3 data sets dans cet projet:
##LTM_Data_2022_8_1.xlsx :
Résumé du contenu: Ce fichier contient des données détaillées sur la qualité de l'eau, principalement des mesures issues de différents points de prélèvement. Il sert à évaluer l'état des masses d'eau à travers divers paramètres chimiques et physiques.
Champs principaux:
SITE_ID : Identifiant unique du site de prélèvement.
PROGRAM_ID : Identifiant du programme de surveillance.
DATE_SMP : Date du prélèvement.
SAMPLE_LOCATION : Description du lieu de prélèvement.
WATERBODY_TYPE : Type de plan d'eau, comme une rivière ou un lac.
SAMPLE_DEPTH : Profondeur de l'échantillon (mètres).
PH_LAB / PH_FLD : Valeurs de pH mesurées en laboratoire ou sur le terrain.
WTEMP_DEG_C : Température de l'eau (en degrés Celsius).
Indicateurs chimiques (comme CA_UEQ_L, SO4_UEQ_L, CL_UEQ_L) : Concentrations de divers éléments chimiques utilisés pour évaluer les caractéristiques chimiques des eaux.
Utilisation: Ces données sont utilisées pour analyser les tendances de la qualité de l'eau dans des masses d'eau spécifiques, identifier des sources de pollution potentielles et fournir une base scientifique pour la gestion des ressources en eau.


##Site_Information_2022_8_1.xlsx :
Résumé du contenu: Ce fichier contient des informations géographiques et topographiques sur chaque site de prélèvement, comme des coordonnées précises, des informations sur les bassins versants, et des données sur les lacs. Cela aide à comprendre le contexte géographique des sites échantillonnés.
Champs principaux:
SITE_ID : Identifiant unique du site de prélèvement, correspondant à celui du fichier LTM_Data.
LATDD / LONDD : Coordonnées géographiques du site de prélèvement (latitude et longitude).
SITE_NAME : Nom du site de prélèvement.
WSHD_AREA_HA : Superficie du bassin versant (hectares).
SITE_ELEV : Altitude du site de prélèvement (mètres).
WSHD_ELEV_AVG / WSHD_ELEV_MIN / WSHD_ELEV_MAX : Altitudes moyenne, minimale et maximale du bassin versant (mètres).
Utilisation: Ces informations sont utilisées pour décrire l'environnement géographique des points d'échantillonnage, facilitant l'analyse spatiale des données de qualité de l'eau. Par exemple, elles permettent d'étudier l'influence de l'altitude et des caractéristiques des bassins versants sur la qualité de l'eau.


##Methods_2022_8_1.xlsx :
Résumé du contenu: Ce fichier inclut des descriptions détaillées sur les méthodes de prélèvement et d'analyse des données de qualité de l'eau. Il précise les standards et les méthodes utilisés, garantissant la cohérence et la comparabilité des analyses.
Champs principaux:
METHOD_ID : Identifiant unique de la méthode.
METHOD_NAME : Nom ou description courte de la méthode.
PARAMETER : Paramètre mesuré spécifique (comme le pH, l'azote ammoniacal, etc.).
METHOD_DESC : Description détaillée de la méthode, y compris les équipements utilisés et les procédures suivies.
Utilisation: Ce fichier garantit la standardisation des analyses de qualité de l'eau, aidant à comprendre et à interpréter les données en fonction des méthodologies appliquées. Pour les analystes et les rédacteurs de rapports, ces informations fournissent des détails techniques sur l'origine des données. ​


#Les étapes de Projet
##Charger des données dans HDFS
Nous utilisons l'outil en ligne de commande hdfs dfs pour effectuer les opérations suivantes sur le système de fichiers distribué HDFS :
Création d'un répertoire : hdfs dfs -mkdir -p /data crée récursivement le répertoire /data si nécessaire.
Copie de fichiers : hdfs dfs -put est utilisé pour copier les fichiers locaux /mnt/data/Methods_2022_8_1.xlsx et /mnt/data/Site_Information_2022_8_1.xlsx vers le répertoire /data dans HDFS.
L'intégrité des données transférées sera vérifiée à l'aide de la commande hdfs dfs -ls /data/


##Flux de données en continu avec Kafka depuis LTM_Data_2022_8_1.xlsx
Dans Kafka, nous produisons des données toutes les 10 secondes en utilisant time.sleep(10). Nous avons également défini une taille de lot de 10 éléments en utilisant batch_size = 10. 

Lorsque nous exécutons notre fichier Python, le message "Message delivered to acidified-water-topic[0]" indique que les données sont bien en cours de transmission et qu'aucune erreur n'a été détectée.

Afin de valider que nos données ont été correctement produites dans Kafka, nous allons utiliser l'outil de consommation de console. La commande :
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic acidified-water-topic --from-beginning


##Intégration les données sur Kafka et HDFS
Cette section montre comment utiliser Spark pour intégrer des données statiques à partir de HDFS et des données en flux provenant de Kafka dans un cadre de données unifié.
La commend:
$ spark-submit     --conf spark.hadoop.fs.defaultFS=hdfs://localhost:8020     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.crealytics:spark-excel_2.12:0.13.5     spark_streaming/integration.py

Tout d'abord, nous initialisons une application Spark via une SparkSession, avec le nom "DataIntegration". Ensuite, les chemins pour Kafka et HDFS sont configurés pour permettre la lecture et l'écriture des données.

###Configuration des sources de données
Configuration de Kafka : Nous avons choisi Kafka comme source de données en flux avec l'adresse du serveur localhost:9092 et le sujet utilisé est acidified-water-topic.

Configuration de HDFS : HDFS est utilisé comme emplacement de stockage des données statiques, avec les chemins methods_path et site_info_path pour les fichiers de données statiques.

Définition des schémas : Afin d'assurer la cohérence des données et la correction des types, nous avons défini les schémas appropriés pour les données statiques et en flux. Les champs incluent l'ID du site d'échantillonnage, le paramètre, la date de prélèvement, le type d'échantillon, etc.

###Lecture et intégration des données
Lecture des données statiques : Les fichiers Excel sont lus à partir de HDFS en utilisant le format com.crealytics.spark.excel. Les données statiques concernant les méthodes d'échantillonnage et les informations sur les sites sont chargées.

Lecture des données en flux : Les données en temps réel sont lues à partir de Kafka, analysées, puis converties en un DataFrame.

Fusion des données : Nous utilisons la méthode join de Spark pour fusionner les données en flux avec les données des méthodes via PROGRAM_ID et PARAMETER. Les informations sur les sites sont également fusionnées à l'aide de l'SITE_ID.

###Stockage des données
Enfin, les données intégrées sont écrites dans un fichier Parquet sur HDFS, garantissant un stockage et une requête efficaces des données. L'écriture des données est réalisée en mode flux et utilise le mécanisme de point de contrôle de Spark pour éviter toute perte de données.


##Analyse des Tendances des Indicateurs de la Qualité de l'Eau
Dans cette section, nous avons utilisé Spark pour traiter et analyser les données de qualité de l'eau. Nous avons d'abord lu le jeu de données intégré, qui a été extrait du système de fichiers distribué (HDFS). Les données ont été lues et stockées au format parquet, garantissant une lecture efficace et flexible des données.

Nous nous sommes concentrés sur deux indicateurs importants de la qualité de l'eau : le pH et la concentration en sulfates. En regroupant les données par site (Site_ID), nous avons calculé la moyenne du pH et de la concentration en sulfates pour chaque site. Les résultats de cette analyse fournissent une représentation visuelle des tendances de la qualité de l'eau pour chaque site.

Les résultats de l'analyse ont été enregistrés dans deux fichiers parquet distincts, dédiés respectivement aux tendances du pH et à la concentration en sulfates. Ces fichiers sont stockés dans le système HDFS à des emplacements spécifiés pour des visualisations futures et des analyses approfondies.


#Choix de stockage des données : Pourquoi utiliser Parquet et NoSQL ?
Lors du traitement de nos données, nous avons choisi d'utiliser le format Parquet pour stocker les données. Parquet est un format de stockage en colonnes, ce qui le rend plus efficace pour l'analyse et les requêtes de données par rapport au stockage en lignes. Le format Parquet prend en charge la compression et le codage des données, ce qui permet de réduire considérablement l'espace de stockage tout en augmentant la vitesse de lecture des données. De plus, il garantit la cohérence des données grâce à une validation stricte des types.
En utilisant le format Parquet lors de l'exécution du script, nous pouvons effectuer des filtrages et des agrégations de données plus rapidement, ce qui est crucial pour les scénarios nécessitant une analyse de données fréquente. Ainsi, le choix du format Parquet représente un bon compromis entre l'efficacité du traitement des données et les coûts de stockage.
Pour l'intégration des données et l'affichage des résultats finaux, nous avons opté pour une base de données NoSQL. NoSQL offre une grande flexibilité et évolutivité, ce qui en fait une solution idéale pour stocker des données semi-structurées ou non structurées. Dans notre ensemble de données, certains champs (comme CA_UEQ_L, SO4_UEQ_L, CL_UEQ_L) peuvent contenir des valeurs nulles ou être absents. Si ces champs sont vides, nous pouvons choisir de ne pas les enregistrer, réduisant ainsi les données redondantes et inutiles. Cela permet également d'éviter des erreurs de calcul ou des biais dus à des données manquantes.
Cependant, si le volume de données est réduit, il est également possible de choisir une base de données SQL pour le stockage. SQL est adapté à la gestion de données structurées et offre des fonctionnalités de requêtes et de manipulation de données plus sophistiquées, ce qui est idéal pour un stockage et une analyse de données de petite envergure.

#Conclusion
Dans ce projet, nous avons intégré et analysé des données de sources variées pour surveiller la chimie des eaux de surface dans quatre régions de l'est des États-Unis de 1980 à 2020, afin d’évaluer l’impact des dépôts de soufre et d’azote sur les écosystèmes aquatiques. Les données comprennent un fichier dynamique reçu via Kafka (LTM_Data_2022_8_1) et deux fichiers statiques depuis HDFS (Methods_2022_8_1, Site_Information_2022_8_1).

Les données intégrées, stockées en Parquet dans HDFS, sont optimisées pour l’analyse. Pour des données plus petites ou bien structurées, une base SQL comme PostgreSQL est envisageable. Nous avons également implémenté un mécanisme de versionnage pour assurer la traçabilité et gérer les incohérences, permettant de revenir à des versions précédentes ou de réintégrer des données via Kafka.

Une base NoSQL (MongoDB, Cassandra) est recommandée pour ce projet, grâce à sa flexibilité et son évolutivité, adaptées aux données semi-structurées et volumineuses.


 
