from pymongo import MongoClient
import pprint

client = MongoClient("mongodb://127.0.0.1:27017")
db = client["topicos_housing"]
collection = db["housing_gold"]

# Conexión
print("✅ Conexión exitosa a MongoDB")
print(f"📊 Total de documentos: {collection.count_documents({})}")
print(f"📋 Colecciones disponibles: {db.list_collection_names()}")

# Primer documento
print("\n📄 Ejemplo de documento:")
pprint.pprint(collection.find_one())

# Casas de alto valor
high_value = collection.count_documents({"is_high_value": "HIGH"})
print(f"\n🏠 Casas de alto valor: {high_value}")

# Promedio de precio por proximidad al océano
pipeline = [
    {"$group": {
        "_id": "$ocean_proximity",
        "promedio_precio": {"$avg": "$median_house_value"}
    }},
    {"$sort": {"promedio_precio": -1}}
]
print("\n🌊 Promedio precio por proximidad al océano:")
for doc in collection.aggregate(pipeline):
    print(f"  {doc['_id']}: ${doc['promedio_precio']:,.0f}")

client.close()