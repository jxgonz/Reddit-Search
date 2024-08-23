from django.shortcuts import render, redirect
from .retrieve import retrieve
import os
# import lucene

def home(request):
  return render(request, "home.html")

def search(request):
  query = None
  results = []
  empty_message = None
  if request.method == "POST":
    query = request.POST.get('query')
    if query.strip() == "":
        empty_message = "No results for an empty query" 
    else:
        directory = os.path.join(os.path.dirname(__file__), 'indexed')
        results = retrieve(directory, query)
    
  return render(request, "home.html", {'query': query, 'results': results})
    
