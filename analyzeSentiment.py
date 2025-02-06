import json
import boto3
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import io
from nltk.sentiment import SentimentIntensityAnalyzer

# 🔥 Initialiser VADER
sia = SentimentIntensityAnalyzer()

# Connexion AWS
dynamodb = boto3.resource("dynamodb", region_name="eu-west-3")
s3 = boto3.client("s3", region_name="eu-west-3")

#Tables DynamoDB
table_reviews = dynamodb.Table("Reviews-dev")
table_restaurants = dynamodb.Table("Restaurants-dev")

BUCKET_NAME = "amplify-bigdata-dev-61ba7-deployment"

def analyze_reviews(restaurantId):
    try:
        response = table_reviews.query(
            IndexName="restaurants_id",
            KeyConditionExpression="restaurants_id = :rid",
            ExpressionAttributeValues={":rid": restaurantId}
        )
        reviews = response.get("Items", [])

        if not reviews:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "Aucune review trouvée pour ce restaurant"})
            }

        text_concat = ""
        sentiment_counts = {"Positif": 0, "Neutre": 0, "Négatif": 0}

        for review in reviews:
            text = review.get("text", "")
            if not text:
                continue
            
            sentiment_scores = sia.polarity_scores(text)
            compound_score = sentiment_scores["compound"]

            sentiment = "Neutre"
            if compound_score >= 0.05:
                sentiment = "Positif"
            elif compound_score <= -0.05:
                sentiment = "Négatif"

            sentiment_counts[sentiment] += 1
            text_concat += " " + text

        wordcloud_buffer = io.BytesIO()
        generate_wordcloud(text_concat, wordcloud_buffer)
        wordcloud_buffer.seek(0)

        wordcloud_s3_path = f"wordclouds/{restaurantId}.png"
        s3.upload_fileobj(wordcloud_buffer, BUCKET_NAME, wordcloud_s3_path, ExtraArgs={"ContentType": "image/png"})

        sentiment_buffer = io.BytesIO()
        generate_sentiment_graph(sentiment_counts, sentiment_buffer)
        sentiment_buffer.seek(0)

        sentiment_s3_path = f"sentiments/{restaurantId}.png"
        s3.upload_fileobj(sentiment_buffer, BUCKET_NAME, sentiment_s3_path, ExtraArgs={"ContentType": "image/png"})

        wordcloud_url = generate_presigned_url(BUCKET_NAME, wordcloud_s3_path)
        sentiment_graph_url = generate_presigned_url(BUCKET_NAME, sentiment_s3_path)

        # ✅ Mise à jour de la table Restaurants-dev avec les URLs générées
        table_restaurants.update_item(
            Key={"restaurants_id": restaurantId},
            UpdateExpression="SET wordcloud_url = :wc, sentiment_graph_url = :sg",
            ExpressionAttributeValues={
                ":wc": wordcloud_url,
                ":sg": sentiment_graph_url
            }
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "wordcloud_url": wordcloud_url,
                "sentiment_graph_url": sentiment_graph_url
            })
        }

    except Exception as e:
        print("Erreur analyse NLP:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Erreur serveur", "details": str(e)})
        }
    
def generate_wordcloud(text, img_buffer):
    wordcloud = WordCloud(width=800, height=400, background_color="white").generate(text)
    
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    
    plt.savefig(img_buffer, format="png", bbox_inches="tight")
    plt.close()

def generate_sentiment_graph(sentiment_counts, img_buffer):
    labels = sentiment_counts.keys()
    sizes = sentiment_counts.values()
    colors = ["#66c2a5", "#fc8d62", "#8da0cb"]
    explode = (0.1, 0, 0)

    plt.figure(figsize=(6, 6))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors, explode=explode, startangle=90)
    plt.title("Répartition des sentiments")
    plt.savefig(img_buffer, format="png", bbox_inches="tight")
    plt.close()

def generate_presigned_url(bucket_name, object_key, expiration=259200):
    try:
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expiration
        )
        return url
    except Exception as e:
        print(f"Erreur lors de la génération de la presigned URL: {e}")
        return None
    
def get_all_restaurant_ids():
    try:
        response = table_reviews.scan(ProjectionExpression="restaurants_id")
        restaurant_ids = list(set(item["restaurants_id"] for item in response.get("Items", [])))
        return restaurant_ids
    except Exception as e:
        print("Erreur récupération des restaurants:", str(e))
        return []
    
if __name__ == "__main__":
    restaurant_ids = get_all_restaurant_ids()

    if not restaurant_ids:
        print("Aucun restaurant trouvé dans la base.")
    else:
        print(f"Analyse des sentiments pour {len(restaurant_ids)} restaurants...")
        for restaurant_id in restaurant_ids:
            print(f"Analyse pour le restaurant: {restaurant_id}")
            result = analyze_reviews(restaurant_id)
            print(json.dumps(result, indent=2, ensure_ascii=False))  # Affichage du JSON formaté