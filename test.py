import pymongo

# MongoDB Atlas connection credentials
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

# def clean_existing_data():
#     for document in cart_collection.find():
#         cleaned_data = clean_data(document)
#         cleaned_cart_collection.insert_one(cleaned_data)
#         print(f"Existing cleaned data inserted: {cleaned_data}")

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

# Execute the trigger-based processing
if __name__ == "__main__":
    print("Listening for changes in the 'carts' collection...")
    process_new_data()

