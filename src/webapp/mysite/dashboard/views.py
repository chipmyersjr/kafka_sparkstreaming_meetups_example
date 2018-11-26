from django.template.loader import get_template
from django.http import HttpResponse
from redis import Redis
import json


def index(request):
    redis_conn = get_redis_connection()

    response = redis_conn.get('CountByResponse')
    response_json = json.loads(response.decode("utf-8"))

    template = get_template("dashboard.html")
    html = template.render(response_json)

    return HttpResponse(html)


def get_redis_connection():
    return Redis(host="172.20.0.7", port=6379)
