import json

def add_sku_images_to_main(data):
    for product in data:
        main_images = set(product.get("mainImages", {}).get("RU", []))  # Convertir en set pour éviter les doublons
        for sku in product.get("sku", []):
            sku_image = sku.get("skuImage", {}).get("RU")
            if sku_image:
                main_images.add(sku_image)  # Ajouter l'URL si elle existe
        
        product["mainImages"]["RU"] = list(set(main_images))  # Appliquer set après l'ajout des images SKU
    return data

# Charger les données JSON
with open("product_data.json", "r", encoding="utf-8") as file:
    products = json.load(file)

# Mettre à jour les images principales
updated_products = add_sku_images_to_main(products)

# Sauvegarder le fichier mis à jour
with open("updated_data.json", "w", encoding="utf-8") as file:
    json.dump(updated_products, file, ensure_ascii=False, indent=4)

print("Mise à jour terminée. Les images SKU ont été ajoutées sans duplication.")
