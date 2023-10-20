"""
Movie Database PTUI.
Team Peacock.
"""


import sys
import psycopg2
import struct
import hashlib
from time import time
from psycopg2 import sql
from sshtunnel import SSHTunnelForwarder
from datetime import datetime, timezone


# movie selection query macro (this is often needed throughout the program, so
# this macro is used instead of copying it everywhere)
MOVIE_QUERY = """SELECT movie.mid, movie.title, 
                ARRAY_AGG(DISTINCT CONCAT(actors.firstname, ' ', actors.lastname)) AS cast_members, 
                ARRAY_AGG(DISTINCT CONCAT(directors.firstname, ' ', directors.lastname)) AS directs,
                ARRAY_AGG(DISTINCT producer_studio.name) AS studios, 
                movie.length, movie.rating, 
                ARRAY_AGG(DISTINCT genre.name) AS genres, 
                ARRAY_AGG(DISTINCT release.releasedate) AS release_dates,
                ROUND(AVG(rates.rating), 3) as user_rating
                FROM movie
                LEFT JOIN release ON movie.mid = release.mid 
                LEFT JOIN makesmovie ON movie.mid = makesmovie.mid 
                LEFT JOIN producer_studio ON makesmovie.prid = producer_studio.prid
                LEFT JOIN actsin ON movie.mid = actsin.mid 
                LEFT JOIN directs ON movie.mid = directs.mid
                LEFT JOIN person actors ON actsin.peid = actors.peid
                LEFT JOIN person directors ON directs.peid = directors.peid
                LEFT JOIN moviegenre ON movie.mid = moviegenre.mid
                LEFT JOIN genre ON moviegenre.gid = genre.gid
                LEFT JOIN rates ON movie.mid = rates.mid"""
                
                
def generate_access_code(password, SALT) -> str:
    """
    Generate access code from salt and password.
    """
    
    pre_code = SALT[:32] + password + SALT[32:64]
    byte_code = bytes(pre_code, "utf-8")
    
    return hashlib.sha3_256(byte_code).hexdigest()


def login(username: str, password: str, conn) -> bool:
    """
    Logs a user into the database.
    
    :return: True for login success or False for login failure
    """
    
    curs = conn.cursor()
    query = f"""SELECT SALT from "User" where username='{username}'"""
    curs.execute(query)
    SALT = curs.fetchall()[0][0]
    
    access_code = generate_access_code(password, SALT)

    query = f"""SELECT * from "User" where username='{username}' AND access_code='{access_code}'"""
    curs.execute(query)

    if curs.rowcount == 1:
        query = f"""UPDATE "User" SET last_access_date = CURRENT_TIMESTAMP WHERE username='{username}' 
                    AND access_code='{access_code}'"""
        curs.execute(query)
        conn.commit()
        curs.close()
        print("Accessed " + username + "'s account on " + str(datetime.now(timezone.utc)))
        return True

    curs.close()
    print("Invalid username or password entered. Please try again")
    return False


def register(username: str, password: str, email: str, firstname: str, lastname: str, SALT: str, conn) -> bool:
    """
    Registers a new user with the database.
    
    :return: True for register success or False for register failure
    """
    
    access_code = generate_access_code(password, SALT)
    curs = conn.cursor()

    while True:
        query = f"""INSERT INTO "User" (username, access_code, email, firstname, lastname, SALT
                ) VALUES ('{username}', '{access_code}', '{email}', '{firstname}', '{lastname}', '{SALT}')"""
        curs.execute(query)
        conn.commit()
        if curs.rowcount == 1:
            curs.close()
            print("User Registered")
            return True
        else:
            curs.close()
            print("The username you entered is already used. Please try again.")


def find_movies(category_code: int, search_term: str, sort_op: int, order_by: str, conn) -> list:
    """
    Finds movies based on search.
    
    :param category_code:
        1 - name
        2 - release date
        3 - cast members
        4 - studio
        5 - genre
        
    :param search_term: movie search word
    :param sort_op:
        0 - alphabetic ordering
        1 - name
        2 - studio
        3 - genre
        4 - release year
        
    :param order_by:
        "a" - ascending
        "d" - descending
        
    The resulting list of movies must show the movie’s name, the cast members,
    the director, the length and the ratings (MPAA and user)

    :param conn: the database connection object
        
    :return: a list of tuples containing movie information (mid, movie name, cast members, studio, length and ratings (MPAA and user))
    """

    if order_by == "a":
        order_by = "ASC"
    else:
        order_by = "DESC"

    query = MOVIE_QUERY

    statement_tuple = (search_term,)
    # default prepared statement tuple

    # quering based on the category code and search term
    if category_code == 1:
        query += """ WHERE movie.title ILIKE {search_term}"""
    elif category_code == 2:
        query += """ WHERE release.releasedate ILIKE {search_term}"""
    elif category_code == 3:
        query += """ WHERE actors.firstname ILIKE {search_term} OR actors.lastname ILIKE {search_term}"""
        statement_tuple = (search_term, search_term)
        # updates statement tuple
        
    elif category_code == 4:
        query += """ WHERE producer_studio.name ILIKE {search_term}"""
    elif category_code == 5:
        query += """ WHERE genre.name ILIKE {search_term}"""
    else:
        return []

    query += f" GROUP BY movie.mid"

    # sort the results based on the sort operation
    if sort_op == 0:
        query += f" ORDER BY movie.title, release_dates {order_by};"
    elif sort_op == 1:
        query += f" ORDER BY movie.title {order_by};"
    elif sort_op == 2:
        query += f" ORDER BY studios {order_by};"
    elif sort_op == 3:
        query += f" ORDER BY genres {order_by};"
    elif sort_op == 4:
        query += f" ORDER BY release_dates {order_by};"
    else:
        return []
        
    stmt = sql.SQL(query).format(
            search_term = sql.Literal("%" + search_term + "%"),
    )

    curs = conn.cursor()
    curs.execute(stmt)
    results = curs.fetchall()
    curs.close()
    return results


def watch_movie(username: str, movie: tuple, conn) -> None:
    """
    Watch a movie.
    """

    curs = conn.cursor()
    movie_id = movie[0]

    # Add the watched movie to the Watched table
    curs.execute("INSERT INTO watches (username, mid, watchdate) VALUES (%s, %s, %s)",
                 (username, movie_id, datetime.now()))
    if curs.rowcount == 1:
        conn.commit()
        print("You have watched the Movie " + str(movie))
    else:
        print("Something went wrong")
    curs.close()


def add_collection(username: str, col_name: str, conn) -> None:
    """
    Add a collection to the database.
    
    :param col_name: collection name
    """

    curs = conn.cursor()
    curs.execute("SELECT max(cid) from collection")
    cid = curs.fetchone()[0]
    cid += 1
    curs.execute("INSERT INTO collection (cid, name, username) values (%s, %s, %s)",
                 (cid, col_name, username))
    if curs.rowcount == 1:
        conn.commit()
        print("Collection " + col_name + " added")
    else:
        print("Something went wrong")
    curs.close()


def del_collection(username: str, collection: tuple, conn) -> None:
    """
    Delete a collection from the database.
    """

    curs = conn.cursor()
    cid = collection[0]

    curs.execute("DELETE FROM collection where cid = %s", (cid,))

    if curs.rowcount == 1:
        conn.commit()
        print("Deleted Collection " + str(collection))
    else:
        print("Something went wrong")

    curs.close()


def rename_collection(username: str, collection: tuple, new_name: str, conn) -> None:
    """
    Renames a collection.
    """

    curs = conn.cursor()
    cid = collection[0]
    curs.execute("UPDATE collection SET name=%s WHERE cid=%s", (new_name, cid))

    if curs.rowcount == 1:
        conn.commit()
        print("Updated Collection " + str(collection) + " to new name " + new_name)
    else:
        print("Something went wrong")

    curs.close()


def add_movie_to_collection(username: str,collection: tuple, movie: tuple, conn) -> None:
    """
    Adds movie to a collection.
    """

    curs = conn.cursor()
    cid = collection[0]
    mid = movie[0]

    curs.execute("INSERT INTO collectionmovies (cid, mid) values (%s, %s)",
                 (cid, mid))

    if curs.rowcount == 1:
        conn.commit()
        print("Updated Collection " + str(collection) + " to have Movie " + str(movie))
    else:
        print("Something went wrong")

    curs.close()


def del_movie_from_collection(username: str, collection: tuple, movie: tuple, conn) -> None:
    """
    Delectes movie from a collection.
    """

    curs = conn.cursor()
    cid = collection[0]
    mid = movie[0]
    curs.execute("DELETE FROM collectionmovies where cid = %s and mid = %s", (cid, mid))

    if curs.rowcount == 1:
        conn.commit()
        print("Deleted Movie " + str(movie) + " from Collection " + str(collection))
    else:
        print("Something went wrong")

    curs.close()


def get_collections(username: str, conn) -> list:
    """
    Gets a list of user collections.
    
    Collection information includes collection ID, collection name, number of movies
        and total watchtime in hours:minutes
    
    :return: a list of tuples containing collection information
    """

    curs = conn.cursor()
    curs.execute(f"""SELECT C.cid, C.name, 0 as "Number of Movies", 0 as "Total Watchtime"
                from collection C where username=%s and 0 = (
                SELECT COUNT(*) from collectionmovies CM where
                CM.cid = C.cid)
                union
                SELECT C.cid, C.name, COUNT(CM.MID) AS "Number of Movies",
                SUM(M.length) AS "Total Watchtime" from collection C,
                collectionmovies CM, movie M  where C.username=%s and CM.cid = C.cid
                and CM.mid = M.mid group by C.cid order by name""", (username, username))
    collections = curs.fetchall()
    curs.close()

    return collections


def find_from_collection(username: str, collection: tuple, sort_op: int, order_by: str, conn) -> list:
    """
    Finds movies in a collection.
    
    :param sort_op:
        0 - alphabetic ordering
        1 - name
        2 - studio
        3 - genre
        4 - release year
        
    :param order_by:
        "a" - ascending
        "d" - descending
    
    :return: a list of tuples containing movie information
    """

    if order_by == "a":
        order_by = "ASC"
    else:
        order_by = "DESC"

    curs = conn.cursor()
    cid = collection[0]

    match sort_op:
        case 0:
            order = f"ORDER BY movie.title, release_dates {order_by}"
        case 1:
            order = f"ORDER BY movie.title {order_by}"
        case 2:
            order = f"ORDER BY studios {order_by}"
        case 3:
            order = f"ORDER BY genres {order_by}"
        case 4:
            order = f"ORDER BY release_dates {order_by}"

    query = f"""{MOVIE_QUERY}
                WHERE movie.mid = (SELECT movie.mid from collectionmovies
                                   WHERE movie.mid = collectionmovies.mid and collectionmovies.cid = {cid})
                GROUP BY movie.mid {order}"""

    curs.execute(query)
    results = curs.fetchall()
    curs.close()
    return results


def rate(username: str, movie: tuple, stars: int, conn) -> None:
    """
    Adds user rating to database
    """

    # rating a movie
    curs = conn.cursor()
    mid = movie[0]
    curs.execute(f"""SELECT * FROM rates WHERE username = '{username}' AND mid = {mid};""")
    print('You have rated the movie!')
    results = curs.rowcount

    if results > 0:
        curs.execute(f"""UPDATE rates SET rating = {stars} WHERE username = '{username}' AND mid = {mid};""")
    else:
        curs.execute("INSERT INTO rates (username, mid, rating) values (%s, %s, %s);", (username, mid, stars))
    curs.close()


def get_friends(username: str, conn) -> list:
    """
    Gets current user's friends
    """

    #gets a current user's friends from a friends table which has two columns: username1 and username2
    curs = conn.cursor()
    curs.execute("SELECT username2 FROM friends WHERE username1=%s", (username,))
    friends = curs.fetchall()
    curs.close()
    return friends


def find_user(username: str, email: str, conn) -> tuple:
    """
    Finds a user by email.
    """

    curs = conn.cursor()

    #find a user by email
    curs.execute("SELECT username, email FROM \"User\" WHERE email=%s", (email,))
    user = curs.fetchone()
    curs.close()
    return user


def follow(username: str, friend: tuple, conn) -> None:
    """
    Follows/adds a user as a friend.
    """

    curs = conn.cursor()
    curs.execute("INSERT INTO friends (username1, username2) VALUES (%s, %s)", (username, friend[0]))

    if curs.rowcount == 1:
        conn.commit()
        print("Followed User ", friend[0])
    else:
        print("Something went wrong")

    conn.commit()
    curs.close()


def unfollow(username: str, friend: tuple, conn) -> None:
    """
    Unfollows/removes a user as a friend 
    """

    curs = conn.cursor()
    curs.execute("DELETE FROM friends WHERE username1=%s AND username2=%s", (username, friend[0]))

    if curs.rowcount == 1:
        conn.commit()
        print("Unfollowed User ", friend[0])
    else:
        print("Something went wrong")

    curs.close()
    
    
def get_collection_count(username: str, conn) -> int:
    """
    Gets number of colections for a user.
    """
    
    curs = conn.cursor()
    curs.execute("""SELECT count(*) FROM collection WHERE
        collection.username=%s""", (username,))
    
    result = curs.fetchone()
    curs.close()
    return int(result[0])
    
    
def get_num_followers(username: str, conn) -> int:
    """
    Gets number of follwers a user has.
    """
    
    curs = conn.cursor()
    curs.execute("""SELECT count(*) FROM friends WHERE 
        friends.username2=%s""", (username,))
        
    result = curs.fetchone()
    curs.close()
    return int(result[0])
    
    
def get_num_following(username: str, conn) -> int:
    """
    Gets number of people a user is following.
    """
    
    curs = conn.cursor()
    curs.execute("""SELECT count(*) FROM friends WHERE
        friends.username1=%s""", (username,))
        
    result = curs.fetchone()
    curs.close()
    return int(result[0])
    
   
def get_user_top_10_movies(username: str, mode: int, conn) -> list:
    """
    Gets a user's top 10 movies
    
    :param mode:
        0 - based on highest rating
        1 - based on most plays
        2 - combination
    """
    
    curs = conn.cursor()
    exec_tuple = (username,)
    # default tuple used in prepared statement
    
    match mode:
        case 0:
            query = f"""{MOVIE_QUERY}
                WHERE rates.username=%s group by movie.mid ORDER BY AVG(rates.rating) DESC LIMIT 10"""
        case 1: 
            query = f"""{MOVIE_QUERY} INNER JOIN watches ON movie.mid = watches.mid
                WHERE watches.username=%s GROUP BY movie.mid ORDER BY count(*) DESC LIMIT 10"""
        case 2:
            query = f"""{MOVIE_QUERY}
                    WHERE movie.mid in (SELECT mov.mid FROM
                    (SELECT movie.mid, 3 AS rating, count(*) AS num FROM movie
                        INNER JOIN watches ON movie.mid = watches.mid
                        WHERE watches.username=%s AND 0 = (
                            SELECT count(*) FROM rates WHERE rates.username=%s
                            AND rates.mid = movie.mid
                            ) GROUP BY movie.mid
                    UNION
                    SELECT movie.mid, AVG(rates.rating), count(*) FROM movie
                        INNER JOIN rates ON movie.mid = rates.mid
                        INNER JOIN watches ON movie.mid = watches.mid
                        WHERE watches.username=%s AND rates.username=%s
                        GROUP BY movie.mid) 
                AS mov ORDER BY mov.rating/5*mov.num DESC LIMIT 10) GROUP BY movie.mid"""
                
            exec_tuple = (username, username, username, username)
            # updates prepared statement tuple

    curs.execute(query, exec_tuple)
    results = curs.fetchall()
    curs.close()
    return results
    
    
def get_overall_top_20_movies(conn) -> list:
    """
    Gets top 20 most popular movies in the last 90 days.
    """
    
    curs = conn.cursor()
    query = f"""{MOVIE_QUERY} INNER JOIN watches ON movie.mid = watches.mid
        WHERE watches.watchdate > CURRENT_DATE-INTERVAL '90 days'
        GROUP BY movie.mid ORDER BY count(*) DESC limit 20"""
        
    curs.execute(query)
    results = curs.fetchall()
    curs.close()
    return results

    
def get_friends_top_20_movies(username: str, conn) -> list:
    """
    Gets top 20 most popular movies among friends.
    """
    
    curs = conn.cursor()
    query = f"""{MOVIE_QUERY} inner join watches w on movie.mid = w.mid
            where w.username in (
                select username1 as name from friends
                where username2=%s
                union
                select username2 as name from friends
                where username1=%s
            )
            group by movie.mid order by count(*) DESC limit 20"""
            
    curs.execute(query, (username, username))
    results = curs.fetchall()
    curs.close()
    return results
    
    
def get_top_5_new_releases(conn) -> list:
    """
    Gets top 5 new releases of the calendar month.
    """
    
    curs = conn.cursor()
    curs.execute(f"""{MOVIE_QUERY}
        inner join watches on movie.mid = watches.mid
        where release.releasedate >= date_trunc('month', current_date)
        group by movie.mid order by count(*) DESC limit 5""")
        
    results = curs.fetchall()
    curs.close()
    return results
    
    
def get_recommended_movies(username: str, conn) -> list:
    """
    Gets recommendations based on user play history and the play history of
    similar users.
    """

    curs = conn.cursor()
    
    #finding the top 5 genres the user likes
    user_top_genres_query = """
        WITH UserTopGenres AS (
            SELECT MG.gid, COUNT(*) AS genrecount
            FROM watches W JOIN moviegenre MG ON W.mid = MG.mid
            WHERE W.username=%s
            GROUP BY MG.gid
            ORDER BY genrecount DESC
            LIMIT 5
        )
        SELECT gid FROM UserTopGenres;
        """
        
    curs.execute(user_top_genres_query, (username,))
    top_genres = [row[0] for row in curs.fetchall()]

    #finding similar users with at least 2 overlapping genres
    similar_users_query = """
        WITH SimilarUsers AS (
            SELECT W.username, COUNT(*) AS overlap_count
            FROM watches W JOIN moviegenre MG ON W.mid = MG.mid
            WHERE MG.gid IN %s AND W.username != %s
            GROUP BY W.username
            HAVING COUNT(*) >= 2
        )
        SELECT username FROM SimilarUsers;
        """
        
    curs.execute(similar_users_query, (tuple(top_genres), username))
    similar_users = [row[0] for row in curs.fetchall()]

    #recommending up to 15 movies based on top genres and similar users
    recommended_movies_query = f"""
        {MOVIE_QUERY} WHERE movie.mid in (
    
            SELECT DISTINCT M.mid
            FROM movie M JOIN moviegenre MG ON M.mid = MG.mid JOIN watches W ON MG.mid = W.mid
            WHERE MG.gid IN %s AND W.username IN %s AND M.mid NOT IN (SELECT mid FROM watches WHERE username=%s)
            LIMIT 15
        
        ) GROUP BY movie.mid ORDER BY movie.rating DESC, movie.title
        """
        
    curs.execute(recommended_movies_query, (tuple(top_genres), tuple(similar_users), username))
    recommendations = curs.fetchall()
    
    return recommendations


#########################################################################
#
#    This ends the database querying section
#
#########################################################################


MOVIE_DISPLAY = (None, "Title:", "Actor(s):", "Director(s):", "Producer/Studio(s): ",
    "Length (min):", "MPAA Rating:", "Genre(s):",
    "Release Date:", "Star Rating:")


COLLECTION_DISPLAY = (None, "Name:", "# of movies:",
    "Total collection play time (min):")


FRIEND_DISPLAY = ("Username:", "Email:")


def data_display(data: list, title: str, disp_op: tuple) -> None:
    """
    Displays formatted like:
    [ tuple(), tuple() ... ]
    This is how movie and collection data should be returned.
    """

    if data is None or data == []:
        print("NO DATA OF TYPE: " + title)
        return

    count = 0

    for elem in data:
        count += 1
        print("\n" + title + " #" + str(count))
        # displays data count

        for dp_index in range(len(elem)):
            if disp_op[dp_index] is not None:
                print(disp_op[dp_index], elem[dp_index])
                # prints data information

    print()
    

def login_query() -> tuple:
    """
    Handles user login prompt.
    """

    print("\nLOGIN:")
    print("======\n")

    print("Enter username:")
    username = input("> ")

    print("Enter password:")
    password = input("> ")

    return username, password


def register_query() -> tuple:
    """
    Handles user sign-up prompt.
    """
    
    pre_SALT = bytearray(struct.pack("f", time()))
    SALT = hashlib.sha3_256(pre_SALT).hexdigest()
    # generate unique user salt value using the current time and SHA3 encoding

    print("\nREGISTER:")
    print("======\n")

    print("Enter username:")
    username = input("> ")

    print("Enter password:")
    password = input("> ")

    print("Enter email:")
    email = input("> ")

    print("Enter your first name:")
    firstname = input("> ")

    print("Enter your last name:")
    lastname = input("> ")

    return username, password, email, firstname, lastname, SALT


def sort_options() -> int:
    """
    Gets user search option based on query.
    
    :return: sort option code
    """

    print("\nPlease select result sort preferences:")
    print("SORT BY")
    print("0 (default) - alphabetic ordering")
    print("1 - name")
    print("2 - studio")
    print("3 - genre")
    print("4 - release year")

    sort_option = input("> ")
    # gets sort option

    print("Order ascending ('a') or descending ('d')?")
    order_by = input("> ")
    # gets sort option

    try:
        sort_option = int(sort_option)

        if order_by != "a" and order_by != "d":
            order_by = "a"
            print("INCORECT ORDERING OPTION - using default (asc) ordering")
    except:
        print("INVALID INPUT - using default sorting/ordering")
        return 0, "a"
        # exits search on invalid entry

    return sort_option, order_by


def rate_prompt(username: str, movie: tuple, conn) -> None:
    """
    Prompts a user for a movie rating.
    """

    print("Would you like to rate this movie? (y/n)")
    rate_op = input("> ")
    # gets rating option ("y" or "n")

    if rate_op.lower() == "y":
        print("Enter star rating (from 1 to 5)")
        stars = input("> ")
        # gets user star rating

        try:
            stars = int(stars)
        except:
            print("INVALID INPUT")
            return
            # exits on invalid entry

        if stars >= 1 and stars <= 5:
            rate(username, movie, stars, conn)
        else:
            print("INVALID INPUT")


def watch_query(username: str, movies: list, conn) -> None:
    """
    Queries a user and plays movie.
    """

    count = 0
    # movie count

    data_display(movies, "MOVIE", MOVIE_DISPLAY)
    # displays movie data

    print("Select a movie by its number to watch it, or enter 0 to exit")

    watch_option = input("> ")
    # gets user watch option

    if watch_option == "0":
        return

    try:
        watch_option = int(watch_option) - 1
        watch_movie(username, movies[watch_option], conn)
    except:
        print("INVALID INPUT")
        return
        # exits search on invalid entry

    rate_prompt(username, movies[watch_option], conn)


def search_movies(username: str, exec_func, conn) -> None:
    """
    Searches for a movie.
    :param exec_func: function to be executed after completed search
    :type exec_func: function
    """

    print("\nSelect search category:")
    print("1 - name")
    print("2 - release date")
    print("3 - cast members")
    print("4 - studio")
    print("5 - genre")
    print("6 - cancel search")

    search_cat = input("> ")
    # gets search category

    if search_cat == "6":
        return

    try:
        search_cat = int(search_cat)
    except:
        print("INVALID INPUT")
        return
        # exits search on invalid entry

    print("\nEnter search term:")

    search_term = input("> ")
    # gets search term

    sort_op, order_by = sort_options()
    # gets sort options

    movies = find_movies(search_cat, search_term, sort_op, order_by, conn)

    if len(movies) == 0:
        print("NO RESULTS FOUND")
        return
        # exits search if no results found

    print("RESULTS FOUND")

    return exec_func(username, movies, conn)
    # runs the passed function on the movie data
    # (basically the Python equivalent of a function pointer)


def play_from_collection_prompt(username: str, cur_collections: list, conn) -> None:
    """
    Play movies from collection prompt.
    """

    print("\nSelect collection number:")
    col_num = input("> ")

    sort_op, order_by = sort_options()
    # gets sort options

    try:
        col_num = int(col_num) - 1
        col_movies = find_from_collection(username, cur_collections[col_num], sort_op, order_by, conn)
    except:
        print("INVALID INPUT")
        return
        # exits search on invalid entry

    if len(col_movies) == 0:
        print("NO RESULTS FOUND")
        return
        # exits search if no results found

    print("\nCOLLECTION MOVIES:")
    data_display(col_movies, "MOVIE", MOVIE_DISPLAY)
    # displays collection movies

    print("Select movie number to play movie, or 0 to play entire collection:")
    col_sel = input("> ")
    # gets collection watch option

    try:
        if col_sel == "0":
            for movie in col_movies:
                watch_movie(username, movie, conn)
                # watches each movie in collection

                rate_prompt(username, movie, conn)
                # allows user to rate each movie watched

        else:
            col_sel = int(col_sel) - 1
            watch_movie(username, col_movies[col_sel], conn)
            # watches selected movie

            rate_prompt(username, col_movies[col_sel], conn)
            # allows user to rate movie

    except:
        print("INVALID INPUT")


def add_collection_prompt(username: str, conn) -> None:
    """
    Add new collection prompt.
    """

    print("Enter new collection name:")
    col_name = input("> ")
    # gets new collection name

    add_collection(username, col_name, conn)


def del_collection_prompt(username: str, cur_collections: list, conn) -> None:
    """
    Delete collection prompt.
    """

    print("Enter collection number you wish to delete:")
    col_num = input("> ")
    # gets collection number to delete

    try:
        col_num = int(col_num) - 1
        del_collection(username, cur_collections[col_num], conn)
    except:
        print("INVALID INPUT")
        return
        # exits search on invalid entry


def rename_collection_prompt(username: str, cur_collections: list, conn) -> None:
    """
    Rename collection prompt.
    """

    print("Enter collection number you wish to rename:")
    col_num = input("> ")
    # gets collection number to delete

    print("Enter new name:")
    new_name = input("> ")
    # gets new collection name

    try:
        col_num = int(col_num) - 1
        rename_collection(username, cur_collections[col_num], new_name, conn)
    except:
        print("INVALID INPUT")
        return
        # exits search on invalid entry


def add_movie_to_collection_query(username: str, movies: list, _) -> str:
    """
    Query executed before adding movie to a collection.
    """

    count = 0
    # movie count

    data_display(movies, "MOVIE", MOVIE_DISPLAY)
    # displays movie data

    print("Select a movie by its number to add it, or enter 0 to exit")

    add_option = input("> ")

    if add_option == "0":
        return

    try:
        add_option = int(add_option) - 1
        return movies[add_option]
    except:
        print("INVALID INPUT")
        return None


def add_movie_to_collection_prompt(username: str, cur_collections: list, conn) -> None:
    """
    Add movie to collection prompt.
    """

    # data_display(cur_collections, "COLLECTIONS", COLLECTION_DISPLAY)
    print("Enter collection number to add a movie to:")
    col_num = input("> ")
    # gets collection number

    print("FIND MOVIE TO ADD TO COLLECTION:")
    movie = search_movies(username, add_movie_to_collection_query, conn)

    if movie is None:
        print("ADD FAILED")
        return

    try:
        col_num = int(col_num) - 1
        add_movie_to_collection(username, cur_collections[col_num], movie, conn)
    except:
        print("INVALID INPUT")
        return
        # exits search on invalid entry


def del_movie_from_collection_prompt(username: str, cur_collections: list, conn) -> None:
    """
    Deletes movie from collection prompt.
    """

    print("Enter collection number:")
    col_num = input("> ")
    # gets collection number

    try:
        col_num = int(col_num) - 1
        collection = cur_collections[col_num]
    except:
        print("INVALID INPUT")
        return
        # exits search on invalid entry

    print("\nFIND MOVIE TO DELETE FROM COLLECTION:")

    col_movies = find_from_collection(username, collection, 0, "a", conn)

    if len(col_movies) == 0:
        print("NO MOVIES IN COLLECTION")
        return

    data_display(col_movies, "MOVIE", MOVIE_DISPLAY)
    print("\nEnter movie number to delete:")
    del_num = input("> ")
    # gets movie deletion number

    try:
        del_num = int(del_num) - 1
        del_movie_from_collection(username, collection, col_movies[del_num], conn)
    except:
        print("INVALID INPUT")
        return
        # exits search on invalid entry


def manage_collections(username: str, conn) -> None:
    """
    Manage collections.
    """

    print("CURRENT COLLECTIONS:")

    cur_collections = get_collections(username, conn)
    data_display(cur_collections, "COLLECTION", COLLECTION_DISPLAY)
    # displays current collection data

    print("\nCollection management options:")
    print("1 - play from collection")
    print("2 - add new collection")
    print("3 - delete collection")
    print("4 - rename collection")
    print("5 - add movie to collection")
    print("6 - delete movie from collection")
    print("7 - quit collection management menu")

    col_op = input("> ")
    # gets collection management option

    if col_op == "1":
        play_from_collection_prompt(username, cur_collections, conn)
    elif col_op == "2":
        add_collection_prompt(username, conn)
    elif col_op == "3":
        del_collection_prompt(username, cur_collections, conn)
    elif col_op == "4":
        rename_collection_prompt(username, cur_collections, conn)
    elif col_op == "5":
        add_movie_to_collection_prompt(username, cur_collections, conn)
    elif col_op == "6":
        del_movie_from_collection_prompt(username, cur_collections, conn)
    elif col_op == "7":
        return
    else:
        print("INVALID INPUT")
        return
        # exits on invalid input


def follow_prompt(username: str, conn) -> None:
    """
    User follow prompt.
    """

    print("Enter email to search for new friend:")
    email = input("> ")
    # gets potential user email

    user = find_user(username, email, conn)

    if user is None:
        print("NO USER FOUND")
        return

    print("USER FOUND:")

    for datapoint in user:
        print(datapoint)
        # prints user information

    print("Would you like to follow this user? (y/n)")
    rate_op = input("> ")
    # gets rating option ("y" or "n")

    if rate_op.lower() == "y":
        follow(username, user, conn)


def unfollow_prompt(username: str, cur_friends: list, conn) -> None:
    """
    User unfollow prompt.
    """

    print("\nEnter friend number to unfollow:")
    del_num = input("> ")
    # gets movie deletion number

    try:
        del_num = int(del_num) - 1
        unfollow(username, cur_friends[del_num], conn)
    except:
        print("INVALID INPUT")
        return
        # exits search on invalid entry


def manage_friends(username: str, conn) -> None:
    """
    Manage friends.
    """

    print("CURRENT FRIENDS:")

    cur_friends = get_friends(username, conn)
    data_display(cur_friends, "FRIEND", FRIEND_DISPLAY)
    # displays current friends

    print("Friend management options:")
    print("1 - follow a new friend")
    print("2 - unfollow a friend")
    print("3 - quit friend management menu")

    friend_op = input("> ")
    # gets friend management option

    if friend_op == "1":
        follow_prompt(username, conn)
    elif friend_op == "2":
        unfollow_prompt(username, cur_friends, conn)
    elif friend_op == "3":
        return
    else:
        print("INVALID INPUT")
        return
        # exits on invalid input   


###########
# PART 3 UI
###########



def show_user_top_10(username: str, conn) -> None:
    """
    Shows user's top 10 movies.
    """
    
    print("\nChoose top 10 selection option:")
    print("0 - based on highest rating")
    print("1 - based on most plays")
    print("2 - combination")
    
    mode = input("> ")
    # gets selection option
    
    try:
        mode = int(mode)    
    except:
        print("INVALID INPUT")
        return
        # exits on invalid input 
    
    print("YOUR TOP 10:")
    data_display(get_user_top_10_movies(username, mode, conn), "MOVIE", MOVIE_DISPLAY)
    # displays top 10 movies
    

def manage_profile(username: str, conn) -> None:
    """
    Allows a user to view profile information.
    """
    
    print("PROFILE INFORMATION:\n")
    
    print("Number of collections:", get_collection_count(username, conn))
    print("Number of followers:", get_num_followers(username, conn))
    print("Number of following:", get_num_following(username, conn))
    # displays user data
    
    print("\nWould you like to view your top 10 movies? (y/n)")
    view_op = input("> ")
    # gets view option ("y" or "n")

    if view_op.lower() == "y":
        show_user_top_10(username, conn)
    
    
def manage_recommendations(username: str, conn) -> None:
    """
    Allows users to view recommendations.
    """
    
    print("\nSelect a recommendation category:")
    print("0 - top 20 most popular movies in the last 90 days")
    print("1 - top 20 most popular movies among my friends")
    print("2 - top 5 new releases of the month")
    print("3 - recommendations based on your play history and the play history of similar users")
    print("4 - quit recommendation manager")
    
    rec_cat = input("> ")
    # gets recommendation category selection
    
    print("RECOMMENDED:")
    
    if rec_cat == "0":
        data_display(get_overall_top_20_movies(conn), "MOVIE", MOVIE_DISPLAY)
    elif rec_cat == "1":
        data_display(get_friends_top_20_movies(username, conn), "MOVIE", MOVIE_DISPLAY)
    elif rec_cat == "2":
        data_display(get_top_5_new_releases(conn), "MOVIE", MOVIE_DISPLAY)
    elif rec_cat == "3":
        data_display(get_recommended_movies(username, conn), "MOVIE", MOVIE_DISPLAY)
    elif rec_cat == "4":
        return
    else:
        print("INVALID INPUT")
        return
        # exits on invalid input  
    

def options_loop(username: str, conn) -> None:
    """
    Main options loop.
    """

    while True:
        # main options loop

        print("\nChoose option below:")
        print("1 - search movies")
        print("2 - manage collections")
        print("3 - manage friends")
        print("4 - my profile")
        print("5 - recommendations")
        print("6 - exit application")

        option = input("> ")
        # gets user option

        if option == "1":
            search_movies(username, watch_query, conn)
        elif option == "2":
            manage_collections(username, conn)
        elif option == "3":
            manage_friends(username, conn)
        elif option == "4":    
            manage_profile(username, conn)
        elif option == "5":
            manage_recommendations(username, conn)
        elif option == "6":
            sys.exit(0)
            # exits application with status 0 

        else:
            print("INVALID OPTION")


def main() -> None:
    """
    Connects to the database using the credentials file
    The first line of the credentials file is the username
    The second line of the credentials file is the password
    
    :return: database connection object
    """

    conn = None
    
    try:
        with open("credentials.txt") as file:
            admin_username = file.readline().strip()
            admin_password = file.readline().strip()

        with SSHTunnelForwarder(
                ('starbug.cs.rit.edu', 22),
                ssh_username=admin_username,
                ssh_password=admin_password,
                remote_bind_address=('localhost', 5432)) as server:

            server.start()
            print("SSH tunnel established on port: " + str(server.local_bind_port))
            params = {
                'database': 'p320_04',
                'user': admin_username,
                'password': admin_password,
                'host': 'localhost',
                'port': server.local_bind_port
            }

            conn = psycopg2.connect(**params)

            print("PEACOCK MOVIES DATABASE")
            print("=======================")
            # title display

            option = ""
            # holds user options

            while True:
                print("\nPlease select from the options below:")
                print("1 - login to existing account")
                print("2 - register new account")
                print("3 - exit application")
                option = input("> ")
                # gets user option input

                if option == "1":
                    username, password = login_query()
                    status = login(username, password, conn)
                    # logs user in

                elif option == "2":
                    username, password, email, firstname, lastname, SALT = register_query()
                    status = register(username, password, email, firstname, lastname, SALT, conn)
                    # registers user and logs into account

                elif option == "3":
                    sys.exit(0)

                else:
                    print("INVALID INPUT")
                    continue

                if status:
                    break
                else:
                    print("REQUEST FAILED -- TRY AGAIN")

            options_loop(username, conn)
            # runs main options loop

    except Exception as e:
        print("Connection Failed")
        print(e)

    finally:
        if conn is not None:
            conn.close()
        print("Goodbye :)")


if __name__ == "__main__":
    main()
