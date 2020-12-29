#!/usr/bin/env python3

# Aplicación web
# Proyecto Grupo 41
# CC3201 Bases de datos

from flask import Flask
from flask import request, render_template
from flask import jsonify

import psycopg2

app = Flask(__name__)

conn = psycopg2.connect(host="cc3201.dcc.uchile.cl", 
	port="5543",
	database="cc3201",
	user="webuser",
	password="<PASSWORD WEBUSER>")


@app.route('/')
def index():
	return render_template('index.html', secure=False)


@app.route('/api/movie_comparison', methods=['GET'])
def get_movie_info():
	artist1 = request.args.get('artist1')
	artist2 = request.args.get('artist2')
	cur = conn.cursor()

	try:
		cur.execute("""
					SELECT m.primaryname AS Name, m.category AS Job, 
					p.primarytitle AS MovieTitle, m.averagerating AS Rating
					FROM imdb.title_basics AS p 
					JOIN (SELECT r.t_tconst, r.averagerating, r.numvotes, n.category, n.primaryname 
						FROM imdb.title_rating AS r 
						JOIN (SELECT t.t_tconst, t.category, d.primaryname 
							FROM imdb.title_principals AS t 
							JOIN (SELECT * FROM imdb.name_basics 
									WHERE primaryname = %s
									OR primaryname = %s)
							AS d ON t.n_nconst = d.nconst)
						AS n ON r.t_tconst = n.t_tconst 
						WHERE r.numvotes > 500)
					AS m ON p.tconst = m.t_tconst 
					WHERE p.titletype = 'movie'
					ORDER BY m.averagerating DESC
					LIMIT 20
					""",
					(artist1, artist2))

		result = [row for row in cur.fetchall()]
	except:
		result = []

	cur.close()
	return jsonify(result)


@app.route('/api/top_actors', methods=['GET'])
def get_top_actors():
	year = request.args.get('year')
	cur = conn.cursor()

	try:
		year = int(year)

		cur.execute("""
					SELECT primaryname, count(*) AS conteo 
					FROM imdb.title_principals AS t 
					JOIN imdb.name_basics AS n 
					ON t.n_nconst = n.nconst 
					WHERE n.birthyear = %s 
					AND (n.primaryprofession LIKE '%%actor%%' OR n.primaryprofession LIKE '%%actress%%') 
					GROUP BY primaryname ORDER BY conteo DESC
					LIMIT 10;
					""",
					(year, ))

		result = [row for row in cur.fetchall()]
	except:
		result = []

	cur.close()
	return jsonify(result)


@app.route('/api/top_movies', methods=['GET'])
def get_top_movies():
	year = request.args.get('year')
	cur = conn.cursor()

	try:
		year = int(year)

		cur.execute("""
					SELECT t.primarytitle AS title, r.averagerating AS rating, r.numvotes AS number_votes FROM imdb.title_basics AS t 
					JOIN imdb.title_rating AS r ON t.tconst = r.t_tconst WHERE t.startYear = %s 
					AND r.numvotes > 20000
					AND t.titletype = 'movie' 
					ORDER BY r.averagerating DESC 
					LIMIT 50
					""",
					(year, ))

		result = [row for row in cur.fetchall()]
	except:
		result = []

	cur.close()
	return jsonify(result)


@app.route('/api/similar_movies', methods=['GET'])
def get_similar_movies():
	movie_name = request.args.get('movie_name')
	cur = conn.cursor()

	try:
		cur.execute("""
					SELECT startyear FROM imdb.title_basics WHERE primarytitle = %s
					""",
					(movie_name, ))


		year = int(cur.fetchone()[0])

		params = {
			"name": movie_name,
			"yearI": (year//10 * 10),
			"yearF": (year//10 * 10 + 10)
		}

		cur.execute("""
					SELECT DISTINCT ON(x.primarytitle) x.primarytitle, x.startyear, y.g_genre AS genre FROM 
						(SELECT p.tconst, primarytitle, startyear FROM 
							(SELECT DISTINCT b.tconst, b.primarytitle FROM 
								(SELECT n.n_nconst, n.t_known_for FROM 
									(SELECT t.tconst, p.n_nconst FROM imdb.title_basics AS t JOIN imdb.title_principals AS p ON t.tconst = p.t_tconst WHERE primarytitle = %(name)s) 
								AS p JOIN imdb.known_for AS n ON p.n_nconst = n.n_nconst WHERE p.tconst <> n.t_known_for) 
							AS t JOIN imdb.title_basics AS b ON t.t_known_for = b.tconst) AS p JOIN 
								(SELECT tconst, startyear FROM imdb.title_basics WHERE startyear BETWEEN %(yearI)s AND %(yearF)s AND primarytitle <> %(name)s) 
							AS m ON m.tconst = p.tconst) AS x JOIN 
								(SELECT t.primarytitle, g.g_genre, g.t_tconst FROM 
									(SELECT g.t_tconst, g.g_genre FROM (SELECT t.tconst, t.primarytitle, g.g_genre FROM imdb.title_basics AS t JOIN imdb.isgenre AS g ON t.tconst = g.t_tconst WHERE t.primarytitle = %(name)s)
								AS t JOIN imdb.isgenre AS g ON t.g_genre = g.g_genre WHERE t.tconst <> g.t_tconst) 
							AS g JOIN imdb.title_basics AS t ON g.t_tconst = t.tconst) 
						AS y ON x.tconst = y.t_tconst
					""",
					params)

		result = [row for row in cur.fetchall()]
	except:
		result = []

	cur.close()
	return jsonify(result)


if __name__ == '__main__':
	app.debug = True
	app.run(port=8080)
