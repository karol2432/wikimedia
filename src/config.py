import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Wczytujemy zmienne z pliku .env
load_dotenv()

def get_local_db():
    """Zwraca obiekt lokalnej bazy danych MongoDB (Docker)."""
    uri = os.getenv("MONGO_LOCAL_URI", "mongodb://admin:adminpassword@localhost:27017/")
    db_name = os.getenv("MONGO_LOCAL_DB", "wikimedia_local")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    return client[db_name]

def get_atlas_db():
    """Zwraca obiekt chmurowej bazy danych MongoDB Atlas."""
    uri = os.getenv("MONGO_ATLAS_URI")
    db_name = os.getenv("MONGO_ATLAS_DB", "wikimedia_cloud")
    if not uri or "tutaj_wkleimy" in uri:
        raise ValueError("Brak poprawnego MONGO_ATLAS_URI w pliku .env!")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    return client[db_name]

if __name__ == "__main__":
    # Szybki test połączenia z lokalną bazą
    db = get_local_db()
    print("Połączono z bazą:", db.name)
    print("Status serwera MongoDB:", db.command("ping"))