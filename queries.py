from database import query


def get_count_shows():
    count = query("SELECT COUNT(id) FROM shows", single=True)
    return count["count"]


def old_people(page=0):
    return query(
        """
        SELECT name
        FROM actors
        WHERE EXTRACT(YEAR from birthday) <=1950 and death is NULL
        LIMIT 10
        OFFSET %s;
    """,
        (page * 10,),
    )


def count_old_people():
    return query(
        """
    SELECT count(name)/10 as count
    FROM actors
    WHERE EXTRACT(YEAR from birthday) <=1950 and death is NULL
    ; """,
        single=True,
    )


def add_email(email):
    query(
        """
     INSERT into genres (email)
     VALUES (%s)
    ;""",
        (email,),
    )


def get_actor_character():
    return query(
        """
    SELECT actors.name as name, string_agg(show_characters.character_name, ', ') as roles
    FROM actors
    LEFT JOIN show_characters on actors.id=show_characters.actor_id
    GROUP BY actors.name
    ORDER by roles DESC
    LIMIT 100   
;"""
    )


def actors_age_death_characters(birthday, death):
    return query(
        """
        SELECT actors.name,
            EXTRACT(YEAR FROM actors.birthday) as birthday, 
            EXTRACT(YEAR FROM actors.death) as death,
            (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM actors.birthday))as age_now,
            (EXTRACT(YEAR FROM actors.death)-EXTRACT(YEAR FROM actors.birthday)) as age_dead,
            COUNT(show_characters.character_name) as number_characters_played
        FROM actors
        LEFT JOIN show_characters ON show_characters.actor_id=actors.id
        WHERE EXTRACT(YEAR FROM actors.birthday) > %s AND EXTRACT(YEAR FROM actors.death) < %s
        GROUP BY actors.name, actors.birthday, actors.death
        ORDER BY actors.name DESC
        ;""",
        (
            birthday,
            death,
        ),
    )


def titles_genres(search_phrase):
    return query(
        """
        SELECT shows.title, string_agg(genres.name, ', ') as genres
        FROM shows
        LEFT JOIN show_genres ON shows.id=show_genres.show_id
        LEFT JOIN genres ON genres.id=show_genres.genre_id
        GROUP BY shows.title
        HAVING shows.title ILIKE %(search_phrase)s
        ;""",
        ({"search_phrase": f"%{search_phrase}%"}),
    )


# table cu toate dramele cu rating >9
# pt fiecare -> overview & list of episodes split by seasontable cu toate dramele cu rating >5
def shows_rating_overview():
    return query(
        """
        SELECT shows.title, ROUND(shows.rating,1) as rating, seasons.title as season, episodes.title as episodes
        FROM shows
        LEFT JOIN show_genres on show_genres.show_id=shows.id
        --LEFT JOIN genres on genres.id=show_genres.genre_id
        LEFT JOIN seasons on shows.id=seasons.show_id
        LEFT JOIN episodes on episodes.season_id=seasons.id
        WHERE show_genres.genre_id =9 AND shows.rating >9
        GROUP BY shows.title, shows.rating, seasons.title, episodes.title
        ORDER BY shows.title
        ;"""
    )


# search by actor
# table with actors, shows they played in
def table_actors_name(name):
    return query(
        """
        SELECT actors.name, string_agg(shows.title, ', ') as shows
        FROM actors
        LEFT JOIN show_characters on show_characters.actor_id=actors.id
        LEFT JOIN shows on show_characters.show_id=shows.id
        GROUP BY actors.name
        HAVING actors.name ILIKE %(name)s
        ;""",
        {"name": f"%{name}%"},
    )


# all shows that have a season called Specials and display them grouped by 1st genre if more than one
def titles_genres_specials():
    return query(
        """
        SELECT shows.title, genres.name as genres
        FROM shows
        LEFT JOIN show_genres ON show_genres.show_id = shows.id
        LEFT JOIN genres ON genres.id=show_genres.genre_id
        LEFT JOIN seasons ON seasons.show_id=shows.id
        WHERE seasons.title IN ('Specials')
        GROUP BY shows.title, genres
        ORDER BY genres ASC
        ;"""
    )


# search by show title, display seasons and season overview (with details html)
def season_details():
    return query(
        """
        SELECT shows.title as show, seasons.title as season, seasons.overview as overview
        FROM shows
        LEFT JOIN seasons on seasons.show_id=shows.id
        GROUP BY shows.title, seasons.title, seasons.overview
        ORDER BY shows.title
        ;"""
    )


# display episode titles, search by season ID and string from episode title
def season_id_title(episode, season_id):
    return query(
        """
        SELECT episodes.title as episode, seasons.id as season_id
        FROM episodes 
        LEFT JOIN seasons ON seasons.id=episodes.season_id
        WHERE episodes.title ILIKE %(episode)s AND seasons.id = %(season_id)s
        GROUP BY episodes.title, seasons.id
        ORDER BY seasons.id
        ; """,
        {"episode": f"%{episode}%", "season_id": int(season_id)},
    )


# 2 separate queries, one shows titles & id in 'action' genre, another no episodes by show id
def show_title_id():
    return query(
        """
        SELECT shows.title, shows.id
        FROM shows
        LEFT JOIN show_genres on show_genres.show_id=shows.id
        LEFT JOIN genres on genres.id=show_genres.genre_id
        WHERE genres.name LIKE 'Action'
        GROUP BY shows.title, shows.id
        ;"""
    )


def show_no_episodes(show_id):
    return query(
        """
        SELECT episodes.title
        FROM episodes
        LEFT JOIN seasons on seasons.id=episodes.season_id
        LEFT JOIN shows on shows.id=seasons.show_id
        WHERE shows.id = %s
        ; """,
        (show_id,),
    )


# all actors who played at least 3 characters, diplay character names & movies on click with definition list
def actors_characters_movies():
    return query(
        """
        SELECT actors.name as actor, show_characters.character_name as character, shows.title as movie
        FROM actors
        LEFT JOIN show_characters ON show_characters.actor_id=actors.id
        LEFT JOIN shows ON shows.id=show_characters.show_id 
        GROUP BY actors.name, show_characters.character_name, shows.title
        ORDER BY actors.name
        ;"""
    )


def actors_by_letter(letter):
    return query(
        """
        SELECT name
        FROM actors
        WHERE name ILIKE %s
        ; """,
        (f"{letter}%",),
    )


# searchbox
# Display a list with show name, episode title
# all shows that contain the specific searched string in the episode title
def ep_title(title):
    return query(
        """
        SELECT shows.title, episodes.title
        FROM shows
        LEFT JOIN seasons ON seasons.show_id=shows.id
        LEFT JOIN episodes ON seasons.id=episodes.season_id
        WHERE episodes.title ILIKE %(title)s
        ; """,
        {
            "title": f"%{title}%",
        },
    )


def no_spaces():
    return query(
        """
        SELECT title 
        FROM shows
        WHERE title NOT LIKE '%% %%'
        ORDER BY title
        ;"""
    )


def get_specials():
    return query(
        """
        SELECT shows.title
        FROM shows
        LEFT JOIN seasons on seasons.show_id=shows.id
        WHERE seasons.title LIKE 'Special%%' AND shows.title LIKE '%%i%%'
        ORDER BY shows.title
        ;"""
    )


def worst_action():
    return query(
        """
        SELECT title, ROUND(rating)::int
        FROM shows
        LEFT JOIN show_genres on show_genres.show_id=shows.id
        LEFT JOIN genres on genres.id=show_genres.genre_id
        WHERE genres.name LIKE 'Action'
        ORDER BY shows.rating ASC
        LIMIT 20
        ;"""
    )


def shows_episodes():
    return query(
        """
        SELECT shows.title, string_agg(episodes.title, '<br/> ') as episodes
        FROM shows
        LEFT JOIN seasons ON seasons.show_id=shows.id
        LEFT JOIN episodes ON episodes.season_id=seasons.id
        GROUP BY shows.title
        ;"""
    )


def living_actors():
    return query(
        """
        SELECT name, birthday, (extract(year from birthday))::int as birthyear
        FROM actors
        WHERE death is Null AND birthday is not null
        ORDER by name
        LIMIT 100
        ;"""
    )


def lifespan():
    return query(
        """
        SELECT ROUND(AVG(EXTRACT(YEAR from death)-EXTRACT(YEAR from birthday)))::int as lifespan
        FROM actors
        WHERE death is not Null
        ;""",
        single=True,
    )


def all_shows():
    return query(
        """
        SELECT title, id 
        FROM shows
        ORDER BY title ASC
        ;"""
    )


def many_roles():
    return query(
        """
        SELECT actors.name, COUNT(shows.id) as no_shows
        FROM actors
        LEFT JOIN show_characters ON show_characters.actor_id=actors.id
        LEFT JOIN shows on shows.id=show_characters.show_id
        GROUP BY actors.name
        HAVING COUNT(shows.id)>=5
        ;"""
    )


def many_roles(phrase):
    return query(
        """
        SELECT name
        FROM actors
        WHERE name ILIKE %(phrase)s
        ;""",
        {
            "phrase": f"% {phrase}%",
        },
    )


def no_characters(number):
    return query(
        """
        SELECT shows.title
        FROM shows
        LEFT JOIN show_characters ON show_characters.show_id=shows.id
        GROUP BY shows.title
        HAVING COUNT(show_characters.character_name) = %(number)s
        ; """,
        {"number": int(number)},
    )


def years_of_life(born, dead):
    return query(
        """
        SELECT actors.name, string_agg(shows.title, ', ') as title, extract(YEAR from actors.birthday) as born, extract(YEAR from actors.death) as dead
        FROM actors
        LEFT JOIN show_characters ON show_characters.actor_id=actors.id
        LEFT JOIN shows on shows.id=show_characters.show_id
        WHERE extract(YEAR from actors.birthday) > %(born)s AND extract(YEAR from actors.death) < %(dead)s
        GROUP BY actors.name, actors.birthday, actors.death
        ; """,
        {
            "born": int(born),
            "dead": int(dead),
        },
    )


def genres_for_dropdown():
    return query(
        """
        SELECT genres.name, string_agg(shows.title, ', ') as titles
        FROM genres
        LEFT JOIN show_genres ON show_genres.genre_id=genres.id
        LEFT JOIN shows ON shows.id=show_genres.show_id
        GROUP BY genres.name
        HAVING COUNT(shows.title)<10 AND COUNT(shows.title) != 0
        ;"""
    )


def get_titles_by_genre(genre):
    return query(
        """
        SELECT shows.title as titles
        FROM genres
        LEFT JOIN show_genres ON show_genres.genre_id=genres.id
        LEFT JOIN shows ON shows.id=show_genres.show_id
        GROUP BY genres.name, shows.title
        HAVING genres.name = %(genre)s
        ; """,
        {"genre": genre},
    )


def get_genres():
    return query(
        """
    SELECT genres.name
    FROM genres
    ;"""
    )


def get_actors_by_genres(genre1, genre2):
    return query(
        """
        SELECT actors.name
        FROM actors
        LEFT JOIN show_characters ON show_characters.actor_id=actors.id
        LEFT JOIN shows ON shows.id=show_characters.show_id
        LEFT JOIN show_genres ON show_genres.show_id=shows.id
        LEFT JOIN genres ON genres.id=show_genres.genre_id
        GROUP BY actors.name, genres.name
        HAVING genres.name IN(%(genre1)s, %(genre2)s) 
        ; """,
        {"genre1": genre1, "genre2": genre2},
    )


def trailer_by_year(year):
    return query(
        """
        SELECT shows.title, string_agg(genres.name, ', ') as genres, shows.trailer
        FROM shows
        LEFT JOIN show_genres ON show_genres.show_id=shows.id
        LEFT JOIN genres on genres.id=show_genres.genre_id
        GROUP BY shows.title, shows.trailer, shows.year
        HAVING EXTRACT(YEAR from year) = %(year)s
        ; """,
        {"year": year},
    )


def actors_same_genre(genre):
    return query(
        """
        SELECT shows.title, string_agg(actors.name, ', ') as actors
        FROM actors
        LEFT JOIN show_characters on show_characters.actor_id=actors.id
        LEFT JOIN shows on shows.id=show_characters.show_id
        LEFT JOIN show_genres on show_genres.show_id=shows.id
        LEFT JOIN genres on genres.id=show_genres.genre_id
        GROUP BY shows.title, genres.name
        HAVING genres.name = %(genre)s AND COUNT(actors.name) > 1  
        ; """,
        {"genre": genre},
    )


def onhover():
    return query(
        """
        SELECT shows.title, 
        COUNT(DISTINCT(seasons.id)) as no_seasons, 
        COUNT(episodes.episode_number) as no_eps
        FROM shows
        JOIN seasons on seasons.show_id=shows.id
        JOIN episodes on episodes.season_id=seasons.id
        GROUP BY shows.title, shows.rating, shows.id
        HAVING shows.rating >7 AND COUNT(seasons.season_number)>3 AND COUNT(episodes.episode_number) <10
        ;"""
    )


def actors_by_shows():
    return query(
        """
        SELECT shows.title, string_agg(actors.name, ', ') as name
        FROM actors
        LEFT JOIN show_characters on show_characters.actor_id=actors.id
        LEFT JOIN shows ON shows.id=show_characters.show_id
        GROUP BY shows.title
        ;"""
    )


def actors_shows():
    return query(
        """
        SELECT actors.name,
            string_agg(shows.title, '@@@') AS titles
        FROM actors
        LEFT JOIN show_characters ON show_characters.actor_id = actors.id
        LEFT JOIN shows ON show_characters.show_id = shows.id
        GROUP BY actors.name
        HAVING count(show_characters.id) > 2
        ORDER BY count(shows.title) DESC
        ;"""
    )


def display_genres_shows(genre):
    return query(
        """
        SELECT genres.name as genre, shows.title as titles, EXTRACT(YEAR FROM shows.year)::int as year
        , ROUND(shows.rating, 1)::varchar(255) as rating
        , (EXTRACT(YEAR FROM shows.year)::int)+ 30 as anniversary
        , (EXTRACT(YEAR FROM shows.year)::int)+ 30 > (EXTRACT(YEAR FROM current_date)) as anniversary_to_follow
        FROM genres 
        JOIN show_genres on show_genres.genre_id=genres.id
        JOIN shows on shows.id=show_genres.show_id
        GROUP BY shows.title, shows.year, shows.rating, genres.name
        HAVING genres.name = %(genre)s
        ORDER BY shows.title
        ;""",
        {"genre": genre},
    )


def get_shows_details(genre):
    return query(
        """
        SELECT shows.title, ROUND(shows.rating, 2)::int as rating, shows.year, genres.name as genre, COUNT(actors.name) as actors
        FROM shows
        JOIN show_characters ON show_characters.show_id=shows.id
        JOIN actors on actors.id=show_characters.actor_id
        JOIN show_genres on show_genres.show_id=shows.id
        JOIN genres on genres.id=show_genres.genre_id
        GROUP BY shows.title, shows.rating, shows.year, genres.name
        HAVING COUNT(actors.name) <20 AND genres.name = %(genre)s
        ;""",
        {"genre": genre},
    )


def get_genres():
    return query(
        """
        SELECT name as genre
        FROM genres
        ;"""
    )


def get_count_shows_genre(genre):
    return query(
        """
        SELECT COUNT(shows.id) as count
        FROM shows
        JOIN show_genres ON shows.id=show_genres.show_id
        JOIN genres ON genres.id=show_genres.genre_id
        WHERE genres.name= %(genre)s
        ; """,
        {"genre": genre},
        single=True,
    )


def get_actors_by_birthday():
    return query(
        """
        SELECT SPLIT_PART(actors.name, ' ', 1) as first_name, actors.birthday
        FROM actors
        ORDER BY actors.birthday
        LIMIT 30
        ;"""
    )


def get_shows_by_actor_name(name):
    return query(
        """
        SELECT SPLIT_PART(actors.name, ' ', 1) as first_name, shows.title
        FROM actors
        JOIN show_characters ON actors.id=show_characters.actor_id
        JOIN shows ON shows.id=show_characters.show_id
        GROUP BY actors.name, shows.title
        HAVING SPLIT_PART(actors.name, ' ', 1) ILIKE %(name)s
        ;""",
        {"name": name},
    )


def get_all_shows_average_rating():
    return query(
        """
    SELECT ROUND(AVG(shows.rating),2)::float AS avg_rating
    FROM shows
    ;""",
        single=True,
    )


def get_shows_and_average_rating():
    return query(
        """
        SELECT shows.title, ROUND(shows.rating, 2)::float as rating, genres.name as genre
        FROM shows
        JOIN show_genres ON shows.id=show_genres.show_id
        JOIN genres ON genres.id=show_genres.genre_id
        GROUP BY shows.title, shows.rating, genres.name
        ORDER BY shows.title
        LIMIT 10
        ;"""
    )


def get_shows_by_genre(genre):
    return query(
        """
        SELECT shows.title, ROUND(shows.rating, 2)::float as rating, genres.name as genre
        FROM shows
        JOIN show_genres ON shows.id=show_genres.show_id
        JOIN genres ON genres.id=show_genres.genre_id
        GROUP BY shows.title, shows.rating, genres.name
        HAVING genres.name = %(genre)s
        ORDER BY shows.rating
        LIMIT 10
        ;""",
        {"genre": genre},
    )


def order_shows_by_rating(criteria="asc"):
    return query(
        f"""
        SELECT shows.title, ROUND(shows.rating,1)::float as rating
        FROM shows
        ORDER BY shows.title {criteria}
        LIMIT 10
        ;"""
    )
