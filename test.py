import pymongo

DB_USER = "sidharthee1905"
DB_PASSWORD = "foodappserver"
DB_NAME = "test"
COLLECTION_NAME = "carts"

connection_string = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@cluster0.av3yj.mongodb.net/{DB_NAME}?retryWrites=true&w=majority"
client = pymongo.MongoClient(connection_string)
db = client[DB_NAME]
cart_collection = db[COLLECTION_NAME]
cleaned_cart_collection = db["cleaned_carts"]

def clean_data(data):
    cleaned = {}
    if "name" in data:
        cleaned["name"] = data["name"]
    else:
        cleaned["name"] = "Unknown Product"     
    if "email" in data:
        cleaned["email"] = data["email"]
    else:
        cleaned["email"] = "Unknown Email" 
    
    return cleaned

# def clean_existing_data():
#     for document in cart_collection.find():
#         cleaned_data = clean_data(document)
#         cleaned_cart_collection.insert_one(cleaned_data)
#         print(f"Existing cleaned data inserted: {cleaned_data}")

# Function to process new data and insert cleaned data
def process_new_data():
    with cart_collection.watch() as stream:
        for change in stream:
            full_document = change.get("fullDocument")
            if full_document:
                cleaned_data = clean_data(full_document)
                cleaned_cart_collection.insert_one(cleaned_data)
                print(f"Cleaned data inserted: {cleaned_data}")

if _name_ == "_main_":
    print("Listening for changes in the 'carts' collection...")
    process_new_data()
