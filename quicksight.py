import json
import boto3
import nltk
from collections import Counter
from decimal import Decimal
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.sentiment import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import TfidfVectorizer

# 📌 Télécharger les ressources NLTK si ce n'est pas déjà fait
nltk.download("punkt")
nltk.download("stopwords")

# 📌 Charger les stopwords en anglais
stop_words = set(stopwords.words("english"))

# 📌 Configuration AWS
s3 = boto3.client("s3", region_name="eu-west-3")
dynamodb = boto3.resource("dynamodb", region_name="eu-west-3")

# 📌 Nom des tables et du bucket S3
table_reviews = dynamodb.Table("Reviews-dev")
table_restaurants = dynamodb.Table("Restaurants-dev")
BUCKET_NAME = "amplify-bigdata-dev-61ba7-deployment"

# 📌 Initialisation de l'analyse des sentiments
sia = SentimentIntensityAnalyzer()

# 📌 Récupération des reviews
response = table_reviews.scan()
reviews_data = response.get("Items", [])

# 📌 Structurer les données pour QuickSight et DynamoDB
restaurants_sentiment = {}

def extract_keywords(text):
    """ Extraire les mots-clés importants en supprimant les stopwords et en utilisant TF-IDF """
    words = word_tokenize(text.lower())
    words = [word for word in words if word.isalnum() and word not in stop_words]
    return words

# Collecte de textes par restaurant pour calcul du TF-IDF
restaurant_texts = {}

for review in reviews_data:
    restaurant_id = review.get("restaurants_id")
    text = review.get("text", "")

    # Calculer le sentiment de la review
    sentiment_score = sia.polarity_scores(text)
    compound_score = sentiment_score["compound"]

    # Initialiser la structure du restaurant si pas encore présent
    if restaurant_id not in restaurants_sentiment:
        restaurants_sentiment[restaurant_id] = {
            "total_reviews": 0,
            "sentiment_scores": [],
            "positive_reviews": 0,
            "neutral_reviews": 0,
            "negative_reviews": 0,
            "word_count": Counter(),
        }
        restaurant_texts[restaurant_id] = []

    # Ajouter le texte pour le TF-IDF
    restaurant_texts[restaurant_id].append(text)

    # Ajouter les stats
    restaurants_sentiment[restaurant_id]["total_reviews"] += 1
    restaurants_sentiment[restaurant_id]["sentiment_scores"].append(compound_score)

    # Classer le sentiment
    if compound_score >= 0.05:
        restaurants_sentiment[restaurant_id]["positive_reviews"] += 1
    elif compound_score <= -0.05:
        restaurants_sentiment[restaurant_id]["negative_reviews"] += 1
    else:
        restaurants_sentiment[restaurant_id]["neutral_reviews"] += 1

# 📌 Appliquer TF-IDF pour récupérer les vrais mots-clés
vectorizer = TfidfVectorizer(stop_words="english", max_features=10)

for restaurant_id, texts in restaurant_texts.items():
    try:
        tfidf_matrix = vectorizer.fit_transform(texts)
        feature_names = vectorizer.get_feature_names_out()
        top_keywords = list(feature_names)
    except ValueError:
        top_keywords = []

    restaurants_sentiment[restaurant_id]["top_keywords"] = top_keywords

# 📌 Fonction pour convertir les valeurs Decimal en float pour JSON
def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError("Type non sérialisable")

# 📌 Mise à jour de la table `Restaurants-dev` avec les nouvelles stats
for restaurant_id, stats in restaurants_sentiment.items():
    avg_sentiment = Decimal(str(sum(stats["sentiment_scores"]) / max(stats["total_reviews"], 1)))

    # Mettre à jour DynamoDB avec ces données
    table_restaurants.update_item(
        Key={"restaurants_id": restaurant_id},
        UpdateExpression="SET avg_sentiment_score = :s, total_reviews = :t, positive_reviews = :p, neutral_reviews = :n, negative_reviews = :ne, top_keywords = :k",
        ExpressionAttributeValues={
            ":s": avg_sentiment,
            ":t": Decimal(stats["total_reviews"]),
            ":p": Decimal(stats["positive_reviews"]),
            ":n": Decimal(stats["neutral_reviews"]),
            ":ne": Decimal(stats["negative_reviews"]),
            ":k": stats["top_keywords"],  # Liste des mots-clés pertinents avec TF-IDF
        }
    )

# 📌 Création du fichier `data.json` pour QuickSight
data_quicksight = [
    {
        "restaurant_id": restaurant_id,
        "total_reviews": stats["total_reviews"],
        "avg_sentiment_score": float(avg_sentiment),  # Convertir Decimal en float
        "positive_reviews": stats["positive_reviews"],
        "neutral_reviews": stats["neutral_reviews"],
        "negative_reviews": stats["negative_reviews"],
        "top_keywords": stats["top_keywords"],
    }
    for restaurant_id, stats in restaurants_sentiment.items()
]

# 📌 Création du fichier `manifest.json` pour QuickSight
manifest_template = {
    "fileLocations": [{"URIs": [f"s3://{BUCKET_NAME}/quicksight/data.json"]}],
    "globalUploadSettings": {"format": "JSON"}
}

# 📌 Sauvegarde et envoi sur S3
manifest_json = json.dumps(manifest_template)
data_json = json.dumps(data_quicksight, ensure_ascii=False, default=convert_decimal).encode("utf-8")

# Upload manifest.json
s3.put_object(Bucket=BUCKET_NAME, Key="quicksight/datasource.manifest", Body=manifest_json)

# Upload data.json
s3.put_object(Bucket=BUCKET_NAME, Key="quicksight/data.json", Body=data_json)

print("✅ Données de sentiment stockées dans DynamoDB et envoyées à S3 pour QuickSight !")