# Data_integration_Bilgenur_Boris_Jiayin_XuanThu
 
# Monitoring of Acidied Surface Waters 
 
**Author:**
Bilgenur OZDEMIR,<br>
Xuan Thu NGUYEN,<br>
Boris Yves NZEUYO DJOKO<br>
, Jiayin CHEN  

**Tuteur:** Lionel Brice SOUOP PEKAM

# Introduction
La surveillance de l'acidification des eaux de surface est un enjeu environnemental crucial. Elle permet d'évaluer l'impact des dépôts atmosphériques d'azote et de soufre sur les écosystèmes aquatiques. Ce projet s'intéresse au développement d'un<br> système d'intégration de données robuste capable de gérer des sources hétérogènes et d'analyser les tendances de la chimie des eaux de surface.

# Objectifs
Ce rapport présente les travaux réalisés pour :
- Comprendre la structure des ensembles de données disponibles (LTM_Data_2022_8_1, Methods_2022_8_1, Site_Information_2022_8_1) stockés dans HDFS.
- Développer une application Kafka pour lire le fichier LTM_Data en continu et l'envoyer par paquets de 10 éléments vers Kafka, avec un intervalle de 10 secondes entre chaque envoi.
- Intégrer les données en streaming provenant de Kafka avec les données statiques stockées dans HDFS afin d'enrichir la base de données.
- Définir, calculer et stocker des indicateurs pertinents pour analyser les tendances de la chimie de l'eau.
- Choisir une architecture de stockage adaptée aux besoins du projet.

**Technologies**
Le projet s'appuie sur les technologies suivantes :
- Spark
- Spark Streaming
- Kafka
- Kafka Connect

# Strucdure de projet
├── database<br>
│   ├── LTM_Data_2022_8_1.xlsx<br>
│   ├── Methods_2022_8_1.xlsx<br>
│   └── Site_Information_2022_8_1.xlsx<br>
├── hdfs_reader<br>
│   ├── __pycache__<br>
│   │   └── read_hdfs.cpython-312.pyc<br>
│   └── read_hdfs.py<br>
├── kafka<br>
│   ├── __pycache__<br>
│   │   └── consumer.cpython-312.pyc<br>
│   └── producer.py<br>
├── metrics_calculator<br>
│   ├── __pycache__<br>
│   │   └── calculate_metrics.cpython-312.pyc<br>
│   └── calculate_metrics.py<br>
├── show_resultat<br>
│   ├── display_Methode.py<br>
│   ├── display_data_integration.py<br>
│   └── show.py<br>
└── spark_streaming<br>
    ├── integration.py<br>
    └── stream_processor.py<br>

Dans ce projet, nous avons une structure de répertoire détaillée avec différentes fonctionnalités :<br>

Dossier **database** :<br>
Ce dossier contient trois fichiers Excel principaux : LTM_Data_2022_8_1.xlsx, Methods_2022_8_1.xlsx, et Site_Information_2022_8_1.xlsx. Ces fichiers servent à stocker les données brutes sur la qualité de l'eau, les informations des sites<br> d'échantillonnage et les méthodes de mesure des différents paramètres.<br>

Dossier **hdfs_reader** :<br>
Contient le script read_hdfs.py, qui sert à lire des données à partir du système de fichiers distribué Hadoop (HDFS). Le fichier read_hdfs.cpython-312.pyc dans le dossier __pycache__ est un fichier de cache pour ce script.<br>

Dossier **kafka** :<br>
Contient le script producer.py, utilisé pour écrire des données dans un topic Kafka.<br>

Dossier **metrics_calculator** :<br>
Contient le script calculate_metrics.py, qui calcule divers indicateurs et statistiques sur la qualité de l'eau.<br>

Dossier **show_resultat** :<br>
Contient trois scripts : display_Methode.py pour afficher les informations sur les méthodes de mesure, display_data_integration.py pour montrer les résultats de l'intégration des données, et show.py pour afficher les résultats globaux.<br>

Dossier **spark_streaming** :<br>
Contient les scripts integration.py et stream_processor.py, qui traitent et intègrent les données en temps réel à l'aide de Spark Streaming.<br>

# Compréhension des données 
On a utilisé 3 data sets dans cet projet:
## LTM_Data_2022_8_1.xlsx :
Résumé du contenu: Ce fichier contient des données détaillées sur la qualité de l'eau, principalement des mesures issues de différents points de prélèvement. Il sert à évaluer l'état des masses d'eau à travers divers paramètres chimiques et physiques.<br>
Champs principaux:<br>
**SITE_ID** : Identifiant unique du site de prélèvement.<br>
**PROGRAM_ID** : Identifiant du programme de surveillance.<br>
**DATE_SMP** : Date du prélèvement.<br>
**SAMPLE_LOCATION** : Description du lieu de prélèvement.<br>
**WATERBODY_TYPE** : Type de plan d'eau, comme une rivière ou un lac.<br>
**SAMPLE_DEPTH** : Profondeur de l'échantillon (mètres).<br>
**PH_LAB / PH_FLD** : Valeurs de pH mesurées en laboratoire ou sur le terrain.<br>
**WTEMP_DEG_C** : Température de l'eau (en degrés Celsius).<br>
**Indicateurs chimiques** (comme CA_UEQ_L, SO4_UEQ_L, CL_UEQ_L) : Concentrations de divers éléments chimiques utilisés pour évaluer les caractéristiques chimiques des eaux.<br>
**Utilisation**: Ces données sont utilisées pour analyser les tendances de la qualité de l'eau dans des masses d'eau spécifiques, identifier des sources de pollution potentielles et fournir une base scientifique pour la gestion des ressources en eau.<br>


## Site_Information_2022_8_1.xlsx :
Résumé du contenu: Ce fichier contient des informations géographiques et topographiques sur chaque site de prélèvement, comme des coordonnées précises, des informations sur les bassins versants, et des données sur les lacs. Cela aide à comprendre le<br> contexte géographique des sites échantillonnés.<br>
Champs principaux:<br>
SITE_ID : Identifiant unique du site de prélèvement, correspondant à celui du fichier LTM_Data.<br>
LATDD / LONDD : Coordonnées géographiques du site de prélèvement (latitude et longitude).<br>
SITE_NAME : Nom du site de prélèvement.<br>
WSHD_AREA_HA : Superficie du bassin versant (hectares).<br>
SITE_ELEV : Altitude du site de prélèvement (mètres).<br>
WSHD_ELEV_AVG / WSHD_ELEV_MIN / WSHD_ELEV_MAX : Altitudes moyenne, minimale et maximale du bassin versant (mètres).<br>
Utilisation: Ces informations sont utilisées pour décrire l'environnement géographique des points d'échantillonnage, facilitant l'analyse spatiale des données de qualité de l'eau. Par exemple, elles permettent d'étudier l'influence de l'altitude et<br> des caractéristiques des bassins versants sur la qualité de l'eau.<br>


## Methods_2022_8_1.xlsx :
Résumé du contenu: Ce fichier inclut des descriptions détaillées sur les méthodes de prélèvement et d'analyse des données de qualité de l'eau. Il précise les standards et les méthodes utilisés, garantissant la cohérence et la comparabilité des<br> analyses.<br>
Champs principaux:<br>
METHOD_ID : Identifiant unique de la méthode.<br>
METHOD_NAME : Nom ou description courte de la méthode.<br>
PARAMETER : Paramètre mesuré spécifique (comme le pH, l'azote ammoniacal, etc.).<br>
METHOD_DESC : Description détaillée de la méthode, y compris les équipements utilisés et les procédures suivies.<br>
Utilisation: Ce fichier garantit la standardisation des analyses de qualité de l'eau, aidant à comprendre et à interpréter les données en fonction des méthodologies appliquées. Pour les analystes et les rédacteurs de rapports, ces informations<br> fournissent des détails techniques sur l'origine des données.<br> ​


# Les étapes de Projet
## Charger des données dans HDFS
Nous utilisons l'outil en ligne de commande hdfs dfs pour effectuer les opérations suivantes sur le système de fichiers distribué HDFS :<br>
Création d'un répertoire : ```bash hdfs dfs -mkdir -p /data ``` crée récursivement le répertoire /data si nécessaire.<br>
Copie de fichiers : ```bash hdfs dfs -put ``` est utilisé pour copier les fichiers locaux /mnt/data/Methods_2022_8_1.xlsx et /mnt/data/Site_Information_2022_8_1.xlsx vers le répertoire /data dans HDFS.<br>
L'intégrité des données transférées sera vérifiée à l'aide de la commande ```bash hdfs dfs -ls /data/ ```<br>


## Flux de données en continu avec Kafka depuis LTM_Data_2022_8_1.xlsx
Dans Kafka, nous produisons des données toutes les 10 secondes en utilisant time.sleep(10). Nous avons également défini une taille de lot de 10 éléments en utilisant batch_size = 10.<br> 

Lorsque nous exécutons notre fichier Python, le message "Message delivered to acidified-water-topic[0]" indique que les données sont bien en cours de transmission et qu'aucune erreur n'a été détectée.<br>

Afin de valider que nos données ont été correctement produites dans Kafka, nous allons utiliser l'outil de consommation de console. La commande :<br>
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic acidified-water-topic --from-beginning<br>


## Intégration les données sur Kafka et HDFS
Cette section montre comment utiliser Spark pour intégrer des données statiques à partir de HDFS et des données en flux provenant de Kafka dans un cadre de données unifié.<br>
La commend:

```bash
$ spark-submit     --conf spark.hadoop.fs.defaultFS=hdfs://localhost:8020     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.crealytics:spark-excel_2.12:0.13.5     spark_streaming/integration.py
```
Tout d'abord, nous initialisons une application Spark via une SparkSession, avec le nom "DataIntegration". Ensuite, les chemins pour Kafka et HDFS sont configurés pour permettre la lecture et l'écriture des données.<br>

### Configuration des sources de données
Configuration de Kafka : Nous avons choisi Kafka comme source de données en flux avec l'adresse du serveur localhost:9092 et le sujet utilisé est acidified-water-topic.<br>

Configuration de HDFS : HDFS est utilisé comme emplacement de stockage des données statiques, avec les chemins methods_path et site_info_path pour les fichiers de données statiques.<br>

Définition des schémas : Afin d'assurer la cohérence des données et la correction des types, nous avons défini les schémas appropriés pour les données statiques et en flux. Les champs incluent l'ID du site d'échantillonnage, le paramètre, la date de<br> prélèvement, le type d'échantillon, etc.<br>

### Lecture et intégration des données
Lecture des données statiques : Les fichiers Excel sont lus à partir de HDFS en utilisant le format com.crealytics.spark.excel. Les données statiques concernant les méthodes d'échantillonnage et les informations sur les sites sont chargées.<br>

Lecture des données en flux : Les données en temps réel sont lues à partir de Kafka, analysées, puis converties en un DataFrame.<br>

Fusion des données : Nous utilisons la méthode join de Spark pour fusionner les données en flux avec les données des méthodes via PROGRAM_ID et PARAMETER. Les informations sur les sites sont également fusionnées à l'aide de l'SITE_ID.<br>

### Stockage des données
Enfin, les données intégrées sont écrites dans un fichier Parquet sur HDFS, garantissant un stockage et une requête efficaces des données. L'écriture des données est réalisée en mode flux et utilise le mécanisme de point de contrôle de Spark pour<br> éviter toute perte de données.


## Analyse des Tendances des Indicateurs de la Qualité de l'Eau
Dans cette section, nous avons utilisé Spark pour traiter et analyser les données de qualité de l'eau. Nous avons d'abord lu le jeu de données intégré, qui a été extrait du système de fichiers distribué (HDFS). Les données ont été lues et stockées au<br> format parquet, garantissant une lecture efficace et flexible des données.<br>

Nous nous sommes concentrés sur deux indicateurs importants de la qualité de l'eau : le pH et la concentration en sulfates. En regroupant les données par site (Site_ID), nous avons calculé la moyenne du pH et de la concentration en sulfates pour<br> chaque site. Les résultats de cette analyse fournissent une représentation visuelle des tendances de la qualité de l'eau pour chaque site.<br>

Les résultats de l'analyse ont été enregistrés dans deux fichiers parquet distincts, dédiés respectivement aux tendances du pH et à la concentration en sulfates. Ces fichiers sont stockés dans le système HDFS à des emplacements spécifiés pour des<br> visualisations futures et des analyses approfondies.<br>


# Choix de stockage des données : Pourquoi utiliser Parquet et NoSQL ?
Lors du traitement de nos données, nous avons choisi d'utiliser le format Parquet pour stocker les données. Parquet est un format de stockage en colonnes, ce qui le rend plus efficace pour l'analyse et les requêtes de données par rapport au stockage<br> en lignes. Le format Parquet prend en charge la compression et le codage des données, ce qui permet de réduire considérablement l'espace de stockage tout en augmentant la vitesse de lecture des données. De plus, il garantit la cohérence<br> des données grâce à une validation stricte des types.<br>
En utilisant le format Parquet lors de l'exécution du script, nous pouvons effectuer des filtrages et des agrégations de données plus rapidement, ce qui est crucial pour les scénarios nécessitant une analyse de données fréquente. Ainsi, le choix du<br> format Parquet représente un bon compromis entre l'efficacité du traitement des données et les coûts de stockage.<br>
Pour l'intégration des données et l'affichage des résultats finaux, nous avons opté pour une base de données NoSQL. NoSQL offre une grande flexibilité et évolutivité, ce qui en fait une solution idéale pour stocker des données semi-structurées ou non<br> structurées. Dans notre ensemble de données, certains champs (comme CA_UEQ_L, SO4_UEQ_L, CL_UEQ_L) peuvent contenir des valeurs nulles ou être absents. Si ces champs sont vides, nous pouvons choisir de ne pas les enregistrer, réduisant ainsi <br>les données redondantes et inutiles. Cela permet également d'éviter des erreurs de calcul ou des biais dus à des données manquantes.<br>
Cependant, si le volume de données est réduit, il est également possible de choisir une base de données SQL pour le stockage. SQL est adapté à la gestion de données structurées et offre des fonctionnalités de requêtes et de manipulation de données <br>plus sophistiquées, ce qui est idéal pour un stockage et une analyse de données de petite envergure.

# Conclusion
Dans ce projet, nous avons intégré et analysé des données de sources variées pour surveiller la chimie des eaux de surface dans quatre régions de l'est des États-Unis de 1980 à 2020, afin d’évaluer l’impact des dépôts de soufre et d’azote sur les<br> écosystèmes aquatiques. Les données comprennent un fichier dynamique reçu via Kafka (LTM_Data_2022_8_1) et deux fichiers statiques depuis HDFS (Methods_2022_8_1, Site_Information_2022_8_1).<br>

Les données intégrées, stockées en Parquet dans HDFS, sont optimisées pour l’analyse. Pour des données plus petites ou bien structurées, une base SQL comme PostgreSQL est envisageable. Nous avons également implémenté un mécanisme de versionnage pour <br>assurer la traçabilité et gérer les incohérences, permettant de revenir à des versions précédentes ou de réintégrer des données via Kafka.<br>

Une base NoSQL (MongoDB, Cassandra) est recommandée pour ce projet, grâce à sa flexibilité et son évolutivité, adaptées aux données semi-structurées et volumineuses.


 
