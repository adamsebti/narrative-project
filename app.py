from flask import Flask, request, jsonify
import flask
import sqlite3
from datetime import datetime, timedelta
import pytz

app = flask.Flask(__name__)


@app.route('/analytics', methods=['GET'])
def get_analytics():
	# Connect to DB
	conn = sqlite3.connect('local')
	cur = conn.cursor()
	# Parse timestamp
	try:
		timestamp = int(request.args.get('timestamp'))
	except ValueError:
		return '', 422
	past_hour = timestamp - 360000
	print(timestamp)
	print(past_hour)
	# Get event data
	cur.execute("SELECT event FROM visits WHERE timestamp > ? AND timestamp < ? ", (past_hour, timestamp))
	data = list(cur)
	clicks = sum([1 for val in data if val[0] == 'click'])
	impressions = sum([1 for val in data if val[0] == 'impression'])
	# Get unique user data
	cur.execute("SELECT COUNT(DISTINCT user_id) FROM visits WHERE timestamp > ? AND timestamp < ? ", (past_hour, timestamp))
	users = list(cur)[0][0]
	# Format result
	results = {
		'unique_users': users if users else 0,
		'clicks': clicks if clicks else 0,
		'impressions': impressions if impressions else 0
	}
	return jsonify(results)


@app.route('/analytics', methods=['POST'])
def post_analytics():
	# Connect to DB
	conn = sqlite3.connect('local')
	cur = conn.cursor()
	# Parse params
	allowed_events = ['click', 'impression']
	event = request.args.get('event')
	if event not in allowed_events:
		return '', 422
	try:
		timestamp = int(request.args.get('timestamp'))
		user_id = int(request.args.get('user_id'))
	except ValueError:
		return '', 422
	# Add to DB
	value = (timestamp, user_id, event)
	cur.execute('insert into visits values (?,?,?)', value)
	conn.commit()
	return '', 204
app.run()


