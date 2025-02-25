import json
import psycopg2

# Charger le fichier JSON existant
with open('/home/iheb/Desktop/th30work/product_data.json', 'r') as file:
    data = json.load(file)

# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    dbname="newdb",
    user="new_user",
    password="iheb",
    host="localhost",
    port=5432
)
cursor = conn.cursor()

# Charger les correspondances de la base de données
cursor.execute("SELECT original_url, cloud_url FROM mimages")
url_mapping = {row[0]: row[1] for row in cursor.fetchall()}

# Liste pour stocker les liens non trouvés
not_found_links = []

# Fonction pour remplacer les liens et collecter les non trouvés
def replace_urls(url_list):
    new_urls = []
    for url in url_list:
        if url in url_mapping:
            new_urls.append(url_mapping[url])
        else:
            new_urls.append(url)
            not_found_links.append(url)
    return new_urls

# Remplacer les liens dans chaque produit
for product in data:
    if 'descImg' in product:
        product['descImg']['RU'] = []
    if 'mainImages' in product:
        product['mainImages']['RU'] = replace_urls(product['mainImages']['RU'])
    if 'descImg' in product:
        product['descImg']['RU'] = replace_urls(product['descImg']['RU'])
    if 'sku' in product:
        for sku in product['sku']:
            if 'skuImage' in sku:
                original_url = sku['skuImage']['RU']
                sku['skuImage']['RU'] = url_mapping.get(original_url, original_url)
                if original_url == sku['skuImage']['RU']:
                    not_found_links.append(original_url)

# Sauvegarder le fichier JSON modifié dans le même fichier
with open('/home/iheb/Desktop/th30work/product_data.json', 'w') as file:
    json.dump(data, file, indent=4, ensure_ascii=False)

# Sauvegarder les liens non trouvés dans un fichier
with open('/home/iheb/Desktop/th30work/notfoundlink.txt', 'w') as file:
    for link in not_found_links:
        file.write(link + '\n')

# Fermer la connexion à la base de données
cursor.close()
conn.close()
