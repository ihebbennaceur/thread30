import psycopg2
import os

# Database connection
conn = psycopg2.connect(
    dbname="new_db2",  # replace with your actual database name
    user="new_user2",   # replace with your database username
    password="nazir",   # replace with your password
    host="localhost",   # replace with your host if necessary
    port="5432"         # default port for PostgreSQL
)

cursor = conn.cursor()

# Step 1: Retrieve product_id, original_url, and their new_location paths
cursor.execute("""
    SELECT 
        product_id, 
        original_url, 
        array_agg(DISTINCT new_location) AS locations
    FROM 
        public.mimages
    GROUP BY 
        product_id, original_url
    HAVING 
        COUNT(DISTINCT new_location) > 1
    ORDER BY 
        product_id, original_url
    LIMIT 1500;
""")

rows = cursor.fetchall()

# Step 2: For each product, choose the first new_location as the "preferred" one
for row in rows:
    product_id = row[0]
    original_url = row[1]
    locations = row[2]  # This is a list of paths where the image is found

    # Choose the first location (or any logic you prefer to pick the "preferred" location)
    preferred_location = locations[0]

    try:
        # Step 3: Update the database to set all new_location values to the preferred one
        cursor.execute("""
            UPDATE public.mimages
            SET new_location = %s
            WHERE product_id = %s 
            AND original_url = %s
            AND new_location != %s;
        """, (preferred_location, product_id, original_url, preferred_location))

        # Commit after each update (or batch commit if necessary)
        conn.commit()
        print(f"Updated product_id {product_id} original_url {original_url} to {preferred_location}")

    except Exception as e:
        # If something goes wrong with the update, rollback the transaction
        conn.rollback()
        print(f"Error updating product_id {product_id} original_url {original_url}: {e}")

    # Step 4: Delete the duplicate image files from disk
    for location in locations:
        if location != preferred_location and os.path.exists(location):
            try:
                os.remove(location)
                print(f"Deleted: {location}")
            except Exception as e:
                print(f"Error deleting {location}: {e}")

# Commit the changes to the database at the end
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("Process completed!")

