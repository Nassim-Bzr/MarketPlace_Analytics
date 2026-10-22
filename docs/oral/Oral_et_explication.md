# MarketPlace Analytics : comprendre le projet et préparer l’oral

Équipe : Medy, Rayan et Nassim.

Format proposé : 9 slides, environ 12 min 30 au total, avec environ 4 min 10 par personne. La limite annoncée par le groupe est de 15 minutes. Les durées comprennent les transitions et une courte démonstration éventuelle. Ce sont des repères à ajuster après une répétition.

## Le projet, simplement

On simule une marketplace qui reçoit des commandes. Le but est de passer automatiquement de ces commandes à des chiffres faciles à consulter : chiffre d’affaires, panier moyen, commissions et classement des vendeurs.

Le parcours est le suivant :

1. Faker invente des informations. Notre code ajoute des règles pour relier correctement vendeurs, produits, clients et commandes.
2. Une API Flask fournit ces données quand Airflow les demande pour une date précise.
3. Airflow écrit les commandes dans MinIO, sous forme de fichiers JSON classés par date. C’est notre copie brute.
4. Le traitement relit MinIO et charge les données dans les tables staging de PostgreSQL.
5. Le SQL alimente le modèle en étoile du DWH, puis les tables de résumés dans analytics.
6. Metabase lit ces tables et affiche les graphiques.

Airflow organise les étapes. Docker Compose lance les services et leur permet de communiquer.

Exemple pédagogique : deux articles à 25 € donnent une commande de 50 €. La commission de 12 % vaut 6 €. Pour une commande annulée ou remboursée, notre règle actuelle ne l’intègre pas au CA des dashboards.

## Les mots à comprendre

| Mot | Explication simple |
|---|---|
| API | Une interface à laquelle on demande des données. Ici, elle simule le backoffice. |
| JSON | Le format de nos fichiers de données brutes. |
| Bucket | Un espace de stockage d’objets dans MinIO, un peu comme un grand dossier. |
| Data lake | Le stockage de fichiers bruts. Ici, MinIO conserve les JSON. |
| DWH | Le Data Warehouse, ou entrepôt de données. Ici, les tables organisées de PostgreSQL. |
| Dimension | Une table qui décrit un vendeur, un produit, un client ou une date. |
| Table de faits | La table des commandes et des montants à analyser. |
| Agrégation | Un résumé calculé à partir des détails, par exemple le CA total d’une journée. |
| DAG | La description des tâches et de leurs dépendances dans Airflow. |
| Hook | Le composant réutilisable qui gère les appels à l’API et son authentification. |
| Asset | Le signal qui indique à Airflow que des données sont à jour. |
| Idempotence | Relancer la même date sans multiplier les commandes ni modifier le résultat métier attendu. |

## Ce qui fonctionne concrètement

La dernière version récupérée du dépôt contenait déjà le code de l’API, des DAGs, du modèle SQL, des tests et du script Metabase. Sur cette machine, on a ensuite préparé le fichier .env, remis Docker en état, démarré les services, chargé les référentiels et l’historique, créé les dashboards et vérifié les résultats.

La réparation de Docker et le nettoyage de Kubernetes concernent la machine locale. Ce ne sont pas des fonctionnalités à présenter comme une partie du pipeline.

Chiffres vérifiés sur l’installation locale le 21 octobre 2026 :

- 200 vendeurs, 2 000 produits et 5 000 clients.
- 31 643 commandes sur 30 jours, du 21 septembre au 20 octobre 2026.
- 33 objets dans Bronze : 30 fichiers de commandes et 3 fichiers de référentiels.
- 24 tests réussis, dont le test d’idempotence sur PostgreSQL.
- 2 dashboards, avec 10 visualisations.
- CA simulé de 5 158 988,17 €, identique dans la table de faits et les trois agrégations.
- Commissions de 619 078,26 €, calculées et arrondies par commande valide pour le CA.

Le README mentionne aussi les résultats d’un autre chargement. Pour cet oral, on garde les chiffres locaux ci-dessus. Tous les montants restent fictifs et le jeu de démonstration ne cherche pas à reproduire exactement les volumes du scénario métier.

## Le point important sur le médaillon

Notre version est hybride : Bronze est dans MinIO, Silver correspond à staging et dwh dans PostgreSQL, et Gold correspond à analytics dans PostgreSQL.

Les buckets MinIO silver et gold existent, mais restent vides. Il ne faut donc pas dire que les trois couches contiennent déjà des fichiers dans MinIO. Leur alimentation, par exemple avec des exports Parquet, reste une évolution à réaliser si le professeur attend les trois couches dans le data lake.

## Consignes trouvées

Aucune consigne spécifique de soutenance n’a été trouvée dans le HTML, le README ou le plan présents dans le dossier. Le sujet renvoie à une Documentation commune qui n’est pas disponible ici. Le groupe a précisé une limite de 15 minutes. On vise environ 12 min 30 pour garder une marge, en supposant que les questions viennent après.

## Répartition de la parole

Cette répartition concerne la présentation. Elle ne prétend pas décrire qui a développé chaque fichier.

| Intervenant | Slides | Partie | Temps total |
|---|---|---|---|
| Medy | 1 à 3 | Besoin, rôle des outils, données fictives | 4 min 10 |
| Rayan | 4 à 6 | Médaillon, modèle SQL, relances sans doublons | 4 min 10 |
| Nassim | 7 à 9 | Airflow, résultats, bilan | 4 min 10 |

## Repères de temps

| Slide | Titre | Qui ? | Durée | Créneau |
|---|---|---|---|---|
| 1 | MarketPlace Analytics | Medy | 1 min 00 s | 0:00 à 1:00 |
| 2 | Le rôle des outils | Medy | 1 min 35 s | 1:00 à 2:35 |
| 3 | Nos données fictives | Medy | 1 min 35 s | 2:35 à 4:10 |
| 4 | Bronze, Silver et Gold | Rayan | 1 min 30 s | 4:10 à 5:40 |
| 5 | Les tables dans PostgreSQL | Rayan | 1 min 20 s | 5:40 à 7:00 |
| 6 | Les relances sans doublons | Rayan | 1 min 20 s | 7:00 à 8:20 |
| 7 | L’automatisation avec Airflow | Nassim | 1 min 30 s | 8:20 à 9:50 |
| 8 | Les résultats sur notre jeu fictif | Nassim | 1 min 40 s | 9:50 à 11:30 |
| 9 | Bilan et suite du projet | Nassim | 1 min 00 s | 11:30 à 12:30 |

## Texte oral, slide par slide

Le texte sert de base. À la répétition, gardez les idées et reformulez les phrases avec vos mots. Les transitions sont incluses dans le temps de chaque slide.

### Slide 1 : MarketPlace Analytics

Medy, environ 1 min 00 s.

Bonjour, on va vous présenter notre projet MarketPlace Analytics. On est partis du cas de Maelys, une marketplace fictive qui met en relation des vendeurs et des clients. Dans le scénario du sujet, l’analyste récupère les commandes à la main pour faire ses rapports Excel. L’idée de notre projet, c’est d’automatiser ce travail et d’obtenir des chiffres faciles à consulter : le chiffre d’affaires, les commissions et les meilleurs vendeurs. On va vous montrer d’où viennent les données, comment on les traite et ce qu’on obtient à la fin.

Transition à dire : Je commence par le rôle des outils, parce qu’on en utilise plusieurs, mais chacun a une fonction assez simple.

À montrer ou à expliquer tranquillement : Exemple à expliquer : on veut pouvoir consulter les ventes du jour sans refaire un export Excel. La commission de 12 % représente la part de la marketplace sur une vente.

### Slide 2 : Le rôle des outils

Medy, environ 1 min 35 s.

Concrètement, Faker sert à inventer nos données : des noms de vendeurs, des clients et des commandes. Flask les rend accessibles par une API, un peu comme si on interrogeait le backoffice d’un vrai site. Airflow s’occupe de lancer les étapes dans le bon ordre. Il appelle l’API et dépose les commandes dans MinIO. MinIO conserve ces données sous forme de fichiers JSON. Ensuite, PostgreSQL les range dans des tables et calcule les indicateurs. Metabase lit les tables pour afficher les graphiques. Docker Compose permet de démarrer tous ces services avec la même configuration. Le parcours des données reste donc assez simple : une source, une copie brute, des tables préparées et des graphiques. On appelle ça un pipeline, c’est-à-dire une suite de traitements qui s’enchaînent.

Transition à dire : Pour que ce pipeline ait quelque chose à traiter, on a d’abord besoin d’une source de données cohérente.

À montrer ou à expliquer tranquillement : En montrant les outils : Docker lance les services sur notre PC. Airflow lance les traitements. MinIO garde les fichiers bruts. PostgreSQL prépare les tables et Metabase les affiche.

### Slide 3 : Nos données fictives

Medy, environ 1 min 35 s.

On utilise Faker en français pour créer les informations fictives. Mais Faker ne fait pas tout : le code choisit aussi les produits, les quantités, les prix et les statuts des commandes. On a 200 vendeurs, 2 000 produits et 5 000 clients. Les commandes pointent vers des éléments qui existent déjà, et le produit correspond bien au vendeur. Ça évite de générer des lignes au hasard qui seraient impossibles à relier ensuite. Autre point important : on fixe une graine de génération. Pour les commandes, elle dépend de la date demandée. Si on redemande la même journée, on retrouve donc les mêmes commandes avec la même configuration. C’est utile pour tester les relances. Par exemple, pour une commande de deux articles à 25 euros, le total est de 50 euros. Avec le taux de 12 % du sujet, la commission vaut 6 euros.

Transition à dire : Je laisse maintenant Rayan expliquer comment on conserve ces données et comment on les organise.

À montrer ou à expliquer tranquillement : Exemple à expliquer : une commande choisit un produit et un client qui existent déjà. Faker aide à créer des noms et des adresses plausibles, tandis que notre code assure les liens entre les données.

### Slide 4 : Bronze, Silver et Gold

Rayan, environ 1 min 30 s.

On a repris le principe de l’architecture médaillon : on passe progressivement des données brutes à des données utilisables pour l’analyse. Bronze, c’est la copie brute. Dans MinIO, on classe les fichiers par type de données puis par date, par exemple orders, puis dt égal à une date. On peut ainsi retrouver une journée précise. Silver correspond ici aux tables PostgreSQL où on type les données et où on les organise. Gold contient les agrégations, comme le chiffre d’affaires par jour ou par vendeur. Il faut préciser un choix de notre version : seule Bronze contient des fichiers dans MinIO. Silver et Gold sont dans PostgreSQL. Les deux buckets MinIO existent, mais ils ne sont pas alimentés. Une évolution possible serait d’y exporter les données préparées, par exemple en Parquet.

Transition à dire : Dans PostgreSQL, on a aussi séparé les tables selon leur rôle.

À montrer ou à expliquer tranquillement : Exemple à expliquer : pour une journée, on conserve le JSON original dans Bronze. On structure ensuite ces données dans PostgreSQL, puis on calcule les indicateurs. Ici, les buckets MinIO Silver et Gold restent vides.

### Slide 5 : Les tables dans PostgreSQL

Rayan, environ 1 min 20 s.

On utilise trois schémas dans PostgreSQL, c’est-à-dire trois espaces pour ranger les tables. Staging reçoit les données relues depuis MinIO. Dwh contient le modèle en étoile. Au centre, fact_orders contient une ligne par commande, avec la quantité, le montant, le statut et la commission. Autour, les quatre dimensions décrivent le vendeur, le produit, le client et la date. Les identifiants permettent de faire les liens. Les clés étrangères empêchent aussi de charger une commande qui référence une dimension absente. Enfin, le schéma analytics contient les résumés que Metabase utilise. Cette séparation nous permet de garder les détails des commandes tout en fournissant des tables simples à interroger pour les graphiques.

Transition à dire : Mais bien ranger les données ne suffit pas : il faut aussi pouvoir relancer le traitement sans les compter deux fois.

À montrer ou à expliquer tranquillement : Exemple à expliquer : pour comparer les ventes par catégorie, on part du montant dans fact_orders et on retrouve la catégorie en passant par la dimension produit.

### Slide 6 : Les relances sans doublons

Rayan, environ 1 min 20 s.

Ce comportement s’appelle l’idempotence. L’idée est simple : si on relance une journée, on ne veut pas doubler ses commandes. Dans MinIO, on réécrit le même fichier au même emplacement. Dans PostgreSQL, on supprime d’abord les lignes de la date choisie, puis on insère celles qu’on vient de préparer. On fait les deux opérations dans une transaction. Si l’insertion échoue, la base peut annuler la suppression au lieu de laisser une journée vide. Les autres dates restent en place. On a aussi un test d’intégration qui exécute deux fois la transformation et compare le résultat. Ça sert quand on corrige un traitement ou quand on veut recharger une journée passée, sans fausser les indicateurs du dashboard.

Transition à dire : Nassim va maintenant présenter comment Airflow enchaîne ces étapes et ce qu’on a vérifié au final.

À montrer ou à expliquer tranquillement : Exemple à expliquer : si on relance le 20 octobre, on remplace les lignes de cette date. On ne rajoute pas une deuxième fois les mêmes commandes.

### Slide 7 : L’automatisation avec Airflow

Nassim, environ 1 min 30 s.

Dans Airflow, un DAG décrit les tâches et leurs dépendances. On en a trois. Le premier met à jour les dimensions. Le deuxième traite les commandes d’une journée : appel de l’API, écriture dans Bronze, relecture du fichier, chargement dans staging et transformation vers la table de faits. Le troisième recalcule les agrégations. Il démarre quand le DAG des commandes signale que les faits sont à jour, grâce à un Asset Airflow. Pour appeler l’API, on a un Hook qui centralise l’authentification et les requêtes. Les identifiants viennent des Connections Airflow configurées par l’environnement. On prévoit aussi des nouvelles tentatives en cas d’erreur temporaire. Dans l’interface, on peut vérifier quelles tâches ont réussi et consulter leurs logs si quelque chose bloque.

Transition à dire : Sur notre installation, on a ensuite chargé un historique pour vérifier le résultat de bout en bout.

À montrer ou à expliquer tranquillement : Sur la capture Airflow : les trois lignes correspondent aux trois DAGs. Les coches vertes indiquent une dernière exécution réussie. Les agrégations attendent la mise à jour des commandes via un Asset.

### Slide 8 : Les résultats sur notre jeu fictif

Nassim, environ 1 min 40 s.

On a chargé 30 jours, du 21 septembre au 20 octobre 2026, soit 31 643 commandes. Les 24 tests passent, dont le test d’idempotence. On a également comparé le chiffre d’affaires de la table de faits avec celui des trois tables d’agrégation : on retrouve le même total, environ 5,16 millions d’euros. Ce sont bien des données fictives. Pour le calcul du CA, notre règle exclut les commandes annulées ou remboursées. Dans Metabase, Executive Summary affiche notamment le CA, le panier moyen et les commissions. Le dashboard Top Sellers permet de comparer les vendeurs. Ici, la courbe reprend les sept dernières journées chargées dans notre installation.

Transition à dire : Pour terminer, voilà ce qu’on retient du projet et ce qu’il reste à améliorer.

Pendant cette slide : montrer la capture intégrée. Si les onglets sont prêts, ouvrir Executive Summary pendant 30 à 40 secondes. Pointer la courbe du CA et les commissions, puis revenir à la slide 9. Si la démo ne répond pas immédiatement, rester sur la capture.

À montrer ou à expliquer tranquillement : Sur la capture Metabase : la courbe montre les variations du CA et les barres comparent les vendeurs. La capture date du 22 octobre. Le bilan chiffré de cette slide reste le chargement vérifié le 21 octobre. Si les services répondent, ouvrir Executive Summary pendant 30 à 40 secondes et montrer aussi les commissions.

### Slide 9 : Bilan et suite du projet

Nassim, environ 1 min 00 s.

Au final, on a un parcours complet qui fonctionne, depuis l’API jusqu’aux dashboards. Les données brutes restent dans MinIO, les tables PostgreSQL structurent les commandes et Metabase permet de consulter les résultats. On peut recharger une date sans multiplier les lignes. Ce qu’on retient surtout, c’est l’importance de garder des données cohérentes et de vérifier les calculs à chaque étape. Il reste des améliorations : ajouter les contrôles qualité configurables, détecter les anomalies commerciales et alimenter les buckets Silver et Gold. La supervision Prometheus et Grafana fait aussi partie des bonus qu’on n’a pas réalisés dans cette version. Merci, on peut maintenant répondre à vos questions.

À montrer ou à expliquer tranquillement : Pour conclure avec vos mots : le parcours fonctionne de bout en bout, mais les contrôles qualité et les exports Silver et Gold dans MinIO restent des améliorations à faire.

## Petite démonstration, si vous en faites une

Le créneau de la slide 8 prévoit 30 à 40 secondes de démonstration, incluses dans les 12 min 30. Les captures Airflow et Metabase intégrées au PowerPoint permettent aussi de présenter le projet sans dépendre du navigateur. Les autres onglets peuvent servir pendant les questions.

1. Préparez la connexion Metabase avant de commencer.
2. Sur Executive Summary, pointez le chiffre d’affaires, la courbe et les commissions.
3. Revenez à la slide 9. Si l’onglet ne répond pas immédiatement, utilisez le graphique du PowerPoint sans passer du temps à déboguer.

Pages utiles sur une machine où les services Docker tournent :

- Executive Summary : http://localhost:3000/dashboard/2
- Top Sellers : http://localhost:3000/dashboard/3
- MinIO : http://localhost:9001/browser/bronze
- Airflow : http://localhost:8080

## Questions probables du professeur

### Pourquoi Faker et une API, au lieu de créer directement des fichiers ?

Faker fournit le jeu fictif. L’API simule la vraie source métier demandée dans le sujet. Elle permet aussi de montrer l’appel HTTP, l’authentification et le Hook Airflow.

### Pourquoi une graine fixe ?

Elle rend la source reproductible : même date et même configuration donnent les mêmes commandes. L’idempotence concerne ensuite le chargement : ces commandes ne doivent pas se dupliquer quand on relance.

### Pourquoi garder les JSON dans MinIO si PostgreSQL contient déjà les commandes ?

On conserve une copie brute, distincte des tables transformées. Les étapes de chargement relisent cette copie. Le DAG complet commence toutefois par rappeler l’API et réécrire le fichier Bronze : il n’existe pas encore de bouton dédié pour rejouer uniquement depuis Bronze.

### Est-ce un médaillon entièrement dans MinIO ?

Dans cette version, c’est une organisation hybride : Bronze dans MinIO, Silver et Gold dans PostgreSQL. Les deux autres buckets sont prêts, mais pas alimentés. Si les trois couches doivent être dans MinIO, les exports restent à ajouter.

### C’est quoi le grain de la table de faits ?

Une ligne correspond à une commande contenant un seul produit dans notre modèle simulé. Plusieurs unités de ce produit peuvent être commandées. Un modèle avec plusieurs produits par commande demanderait des lignes de commande distinctes.

### Que se passe-t-il si une transformation échoue après le DELETE ?

Le DELETE et l’INSERT sont dans une même transaction. En cas d’erreur, la transaction échoue et la suppression n’est pas validée. Les autres dates restent en place.

### Qu’est-ce qui compte dans le CA ?

Le code actuel exclut cancelled et refunded. Il inclut les autres statuts, y compris pending. Cette définition est une règle du MVP à confirmer avec un vrai responsable métier. On ne présente pas ce calcul comme une règle comptable universelle.

### Pourquoi la somme des commissions ne vaut-elle pas forcément exactement 12 % du CA total ?

On calcule puis arrondit la commission à deux décimales pour chaque commande. La somme de ces arrondis peut différer légèrement de l’arrondi de 12 % du total global.

### Comment évitez-vous les références vers des dimensions absentes ?

Le générateur relie les identifiants existants. PostgreSQL ajoute des clés étrangères. Le pipeline possède aussi une étape ensure_dimensions qui crée les clés manquantes avec des valeurs de remplacement, avant le chargement des faits.

### Qu’avez-vous testé ?

Les imports et la structure des DAGs, leurs réglages, le Hook et son comportement, ainsi que l’idempotence de la transformation sur PostgreSQL. On a aussi exécuté les DAGs et comparé le total du CA entre faits et agrégations. Les 24 tests ne remplacent pas une validation complète pour une mise en production.

### Qu’est-ce qui manque ?

Le DataQualityOperator configurable et son branching, la détection d’anomalies, Prometheus et Grafana, ainsi que les exports Silver et Gold dans MinIO. Des contraintes et contrôles existent déjà, mais ces bonus spécifiques ne sont pas implémentés.

### Pourquoi ne pas utiliser Spark ou dbt ?

Le sujet demande un MVP court et les transformations SQL restent simples à ce volume. Airflow, MinIO et PostgreSQL suffisent pour démontrer le fonctionnement demandé.

## Avant de passer

- Faire une répétition chronométrée à trois, avec les deux passages de parole.
- Savoir expliquer l’exemple de 50 € et de 6 € de commission.
- Savoir retrouver un fichier dans Bronze et une exécution réussie dans Airflow.
- Vérifier que Metabase est connecté avant de démarrer l’oral.
- Assumer les limites de la version actuelle, surtout les buckets Silver et Gold vides.

## Sources utilisées pour préparer les supports

Cahier des charges HTML, README.md, docs/PLAN.md, docker-compose.yml, api/app.py, init-db/schema.sql, DAGs et SQL du dépôt, plugins/marketplace_io.py, plugins/hooks/marketplace_api_hook.py et tests du projet.

Les résultats chiffrés viennent des requêtes exécutées sur l’installation locale le 21 octobre 2026 et du journal des tests. Le graphique du PowerPoint reprend les sept derniers jours de analytics.daily_summary. Les captures Airflow et Metabase ont été prises le 22 octobre 2026. Les trois intervenants totalisent environ 12 min 30, avec des durées indicatives et une limite de 15 minutes.
