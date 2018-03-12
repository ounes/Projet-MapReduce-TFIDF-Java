# Tp TfIdf
## Job 1 complete : calcul du nombre d'occurences des mots dans des documents

Input : dossier contenant des fichiers textes (`mobydick.txt` et `r_crusoe`)

(mise en cache du fichier `stopwords_en.txt` pour skip des mots)

Filtrer avec les critères suivants :

- Mise en minuscule de tous les mots
- Filtrer les mots inutiles contenu dans un fichier `stopwords_en.txt`
- Suppression des ponctuations et caractères numériques
- Suppression des mots de moins de 3 caractères

Sortie : fichier contenant des lignes avec clé `<docID mot>` et valeur `<wordcount>`

## Job 2 complete : calcul du nombre total de mots par document et insertion en fin de lignes
- Input : fichier de sortie du job 1
- Output : Fichier contenant des lignes avec clé `<docID mot>` et valeur `<wordcount wordperdoc>`
## Job 3 complete : calcul du tf_idf pour chaque mot
- Input : fichier de sortie du job 2
- Output : fichier contenant des lignes avec clé `<docID mot>` et valeur `<tf_idf>`
