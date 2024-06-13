import json
import sys
import sqlite3


files = {
        "allrecipes": "data/allrecipes-recipes.json", 
        "bbccouk": "data/bbccouk-recipes.json", 
        "cookstr": "data/cookstr-recipes.json",
        "epicurious": "data/epicurious-recipes.json"}

def main(argv):
    conn = sqlite3.connect("recipes.db")
    conn.execute("PRAGMA foreign_keys = 1")
    cur = conn.cursor()
    create_tables(cur)
    load_json_to_db(cur)
    conn.commit()
    

def create_tables(cur):
    cur.execute("""
                    CREATE TABLE IF NOT EXISTS recipe (
                        id INTEGER PRIMARY KEY,
                        title TEXT,
                        time INTEGER,
                        url TEXT,
                        photourl TEXT
                    )
                """)
    cur.execute("""
                    CREATE TABLE IF NOT EXISTS tag (
                        id INTEGER PRIMARY KEY,
                        name TEXT
                    )
                """)
    cur.execute("""
                    CREATE TABLE IF NOT EXISTS ingredient_recipe (
                        recipe_id INTEGER,
                        description TEXT,
                        FOREIGN KEY (recipe_id) REFERENCES recipe (id)
                    )
                """)
    cur.execute("""
                    CREATE TABLE IF NOT EXISTS tag_recipe (
                        tag_id INTEGER,
                        recipe_id INTEGER,
                        FOREIGN KEY (tag_id) REFERENCES tag (id),
                        FOREIGN KEY (recipe_id) REFERENCES recipe (id)
                    )
                """)
    cur.execute("""
                    CREATE TABLE IF NOT EXISTS instruction_recipe (
                        recipe_id INTEGER,
                        ord INTEGER,
                        description TEXT,
                        FOREIGN KEY(recipe_id) REFERENCES recipe(id)
                    )
                """)


def load_json_to_db(cur):
    load_file_to_db(
            cur,
            "allrecipes",
            {"title": "title", "time": "total_time_minutes", "url": "url", "photourl": "photo_url"},
            lambda recipe : recipe["ingredients"],
            lambda recipe : recipe["instructions"]
    )
    load_file_to_db(
            cur, 
            "bbccouk", 
            {"title": "title", "time": "total_time_minutes", "url": "url", "photourl": "photo_url"},
            lambda recipe : recipe["ingredients"],
            lambda recipe : recipe["instructions"]
    )
    load_file_to_db(
            cur,
            "epicurious",
            {"title": "hed", "url": "url"},
            lambda recipe : recipe["ingredients"],
            lambda recipe : recipe["prepSteps"]
    )
    load_file_to_db(
            cur,
            "cookstr",
            {"title": "title", "time": "total_time", "url": "url", "photourl": "photo_url"},
            lambda recipe : recipe["ingredients"],
            lambda recipe : recipe["instructions"]
    )


def load_file_to_db(cur, website_name, recipe_parameters, get_ingredients, get_instructions):
    print(f"Inserting recipes from {website_name}")
    err_count = 0
    for (i, recipe) in enumerate(load_jsons(files[website_name])):
        try:
            keys = recipe_parameters.keys()
            desc = f"({', '.join(keys)})"
            placeholders = f"({', '.join('?' for key in keys)})"
            values = tuple([recipe[recipe_parameters[key]] for key in keys])
            cur.execute(f"INSERT INTO recipe {desc} VALUES {placeholders}", values)
            recipe_id = cur.lastrowid
            ingredient_values = [(recipe_id, ingredient) for ingredient in get_ingredients(recipe)]
            cur.executemany("INSERT INTO ingredient_recipe (recipe_id, description) VALUES (?, ?)", ingredient_values)
            instruction_values = [(recipe_id, i+1, instruction) for (i, instruction) in enumerate(get_instructions(recipe))]
            cur.executemany("INSERT INTO instruction_recipe (recipe_id, ord, description) VALUES (?, ?, ?)", instruction_values)
            if ((i+1) % 1000 == 0):
                print(f"Inserted {i+1} recipes")
        except:
            err_count += 1
    print(f"Couldn't insert {err_count} recipes due to key errors")

def load_jsons(file):
    with open(file, "r") as file:
        for line in file:
            yield json.loads(line)


def output_keys():
    for name in names:
        with open(f"data/{name}-recipes.json", "r") as input_file, open(f"data/{name}-keys", "w") as output_file:
            keys = set()
            for line in input_file:
                recipe = json.loads(line)
                for key in recipe.keys():
                    if key not in keys:
                        output_file.write(key + "\n")
                        keys.add(key)


def get_unique_values(keyword, data):
    values = set()
    for recipe in data:
        if keyword in recipe.keys() and str(recipe[keyword]) not in values:
            yield str(recipe[keyword])
            values.add(str(recipe[keyword]))


def get_values(keyword, data):
    for recipe in data:
        yield(str(recipe[keyword]).replace("\n", " // // "))


if __name__ == "__main__":
    main(sys.argv)
