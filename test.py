import os
import pymongo
from flask import Flask

app = Flask(__name__)

# MongoDB Atlas connection credentials from environment variables
DB_USER = "sidharthee1905"
DB_PASSWORD = "foodappserver"
DB_NAME = "test"
COLLECTION_NAME = "carts"

# MongoDB Atlas connection string
connection_string = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@cluster0.av3yj.mongodb.net/{DB_NAME}?retryWrites=true&w=majority"

# Connect to MongoDB Atlas
client = pymongo.MongoClient(connection_string)
db = client[DB_NAME]
cart_collection = db[COLLECTION_NAME]
cleaned_cart_collection = db["cleaned_carts"]

# Function to clean the data
def clean_data(data):
    cleaned = {}
    
    # Extract only 'name' and 'email' fields
    if "name" in data:
        cleaned["name"] = data["name"]
    else:
        cleaned["name"] = "Unknown Product"  # Default name if missing
    
    if "email" in data:
        cleaned["email"] = data["email"]
    else:
        cleaned["email"] = "Unknown Email"  # Default email if missing
    
    return cleaned

# Function to process new data and insert cleaned data
def process_new_data():
    # Use MongoDB Change Stream to listen to changes in the 'carts' collection
    with cart_collection.watch() as stream:
        for change in stream:
            # When a document is inserted/updated, get the full document
            full_document = change.get("fullDocument")
            if full_document:
                # Clean the data
                cleaned_data = clean_data(full_document)
                
                # Insert the cleaned data into the 'cleaned_carts' collection
                cleaned_cart_collection.insert_one(cleaned_data)
                print(f"Cleaned data inserted: {cleaned_data}")

@app.route('/')
def home():
    return "Listening for changes in the 'carts' collection..."

if __name__ == "__main__":
    print("Listening for changes in the 'carts' collection...")
    process_new_data()
    app.run(host="0.0.0.0", port=10000)
