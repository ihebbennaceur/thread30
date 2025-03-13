import os
import psycopg2

# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    dbname="new_db2",
    user="new_user2",
    password="nazir",
    host="localhost",
    port="5432"
)

# Création d'un curseur pour exécuter les requêtes SQL
cursor = conn.cursor()

# Récupération des fichiers WebP depuis la base de données
cursor.execute("SELECT id, new_location FROM mimage WHERE new_location LIKE '%.webp'")
rows = cursor.fetchall()

for row in rows:
    image_id, old_path = row
    new_path = old_path.replace(".webp", ".jpg")  # Nouveau nom de fichier

    # Vérifier si le fichier existe avant de le renommer
    if os.path.exists(old_path):
        os.rename(old_path, new_path)  # Renommer le fichier
        print(f"Renommé : {old_path} -> {new_path}")

        # Mettre à jour le chemin dans la base de données
        cursor.execute("UPDATE mimage SET new_location = %s WHERE id = %s", (new_path, image_id))
    else:
        print(f"Fichier introuvable : {old_path}")

# Validation des modifications et fermeture de la connexion
conn.commit()
cursor.close()
conn.close()

print("✅ Renommage terminé et base de données mise à jour.")
