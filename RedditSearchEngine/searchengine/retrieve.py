
import os
import json
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID, NUMERIC, STORED
from whoosh.analysis import StemmingAnalyzer
from whoosh.qparser import QueryParser


def create_index(dir, file):
    if not os.path.exists(dir):
        os.mkdir(dir)
    
    schema = Schema(
        PostID=ID(stored=True),
        Subreddit=TEXT(stored=True, analyzer=StemmingAnalyzer()),
        Title=TEXT(stored=True, analyzer=StemmingAnalyzer()),
        Text=TEXT(stored=True, analyzer=StemmingAnalyzer()),
        Author=ID(stored=True),
        Score=NUMERIC(stored=True),
        UpvoteRatio=NUMERIC(stored=True),
        URL=ID(stored=True),
        Permalink=ID(stored=True),
        Comments=TEXT(stored=True, analyzer=StemmingAnalyzer()),
        LinkTitles=TEXT(stored=True, analyzer=StemmingAnalyzer()),
        CreatedAt=ID(stored=True)
    )
    
    ix = create_in(dir, schema)
    writer = ix.writer()

    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
        for entry in data:
            post_id = entry['id']
            subreddit = entry['subreddit']
            title = entry['title']
            text = entry['text']
            author = entry['author']
            score = entry['score']
            upvote_ratio = entry['upvote_ratio']
            url = entry['url']
            permalink = entry['permalink']
            comments = ' '.join([comment for comment in entry['comments'] if comment])
            link_titles = ' '.join([lt for lt in entry['link_titles'] if lt])
            created_at = entry['created_at']

            writer.add_document(
                PostID=str(post_id),
                Subreddit=subreddit,
                Title=title,
                Text=text,
                Author=author,
                Score=score,
                UpvoteRatio=upvote_ratio,
                URL=url,
                Permalink=permalink,
                Comments=comments,
                LinkTitles=link_titles,
                CreatedAt=str(created_at)
            )
    writer.commit()


def retrieve(storedir, query):
    ix = open_dir(storedir)
    searcher = ix.searcher()

    parser = QueryParser("Text", ix.schema)
    parsed_query = parser.parse(query)

    results = searcher.search(parsed_query, limit=10)
    topkdocs = []
    for hit in results:
        topkdocs.append({
            "score": hit.score,
            "subreddit": hit["Subreddit"],
            "title": hit["Title"],
            "text": hit["Text"],
            "comments": hit["Comments"]
        })

    return topkdocs


# directory = 'DATA/'

# for f in os.listdir(directory):
#     path = os.path.join(directory, f)
#     if os.path.isfile(path):
#         create_index(f'indexed/{f}/', path)

# query = input("Enter to Search: ")

# results = retrieve('reddit_index/', query)

# for result in results:
#     print(result)

# with open('result.json', 'w', encoding='utf-8') as f:
#     f.write('[\n')
#     for i, item in enumerate(results):
#         f.write('\t')
#         json.dump(item, f, ensure_ascii=False)
#         if i < len(results) - 1:
#             f.write(',\n')
#     f.write('\n]')










# import logging, sys
# logging.disable(sys.maxsize)

# import lucene
# import os
# import json
# from org.apache.lucene.store import MMapDirectory, SimpleFSDirectory, NIOFSDirectory
# from java.nio.file import Paths
# from org.apache.lucene.analysis.standard import StandardAnalyzer
# from org.apache.lucene.document import Document, Field, FieldType
# from org.apache.lucene.queryparser.classic import QueryParser, MultiFieldQueryParser
# from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader, MultiReader
# from org.apache.lucene.search import IndexSearcher, BoostQuery, Query
# from org.apache.lucene.search.similarities import BM25Similarity


# def create_index(dir, file):
#     if not os.path.exists(dir):
#         os.mkdir(dir)
        
#     store = SimpleFSDirectory(Paths.get(dir))
#     analyzer = StandardAnalyzer()
#     config = IndexWriterConfig(analyzer)
#     config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
#     writer = IndexWriter(store, config)
    
#     metaType = FieldType()
#     metaType.setStored(True)
#     metaType.setTokenized(False)
    
#     contextType = FieldType()
#     contextType.setStored(True)
#     contextType.setTokenized(True)
#     contextType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
    
#     f = open(file, 'r', encoding='utf-8')
#     data = json.load(f)
    
#     for entry in data:
#         post_id = entry['id']
#         subreddit = entry['subreddit']
#         title = entry['title']
#         text = entry['text']
#         author = entry['author']
#         score = entry['score']
#         upvote_ratio = entry['upvote_ratio']
#         url = entry['url']
#         permalink = entry['permalink']
#         comments = ' '.join([comment for comment in entry['comments'] if comment])
#         link_titles = ' '.join([lt for lt in entry['link_titles'] if lt])
#         created_at = entry['created_at']
        
#         doc = Document()
#         doc.add(Field('PostID', str(post_id), metaType))
#         doc.add(Field('Subreddit', str(subreddit), contextType))
#         doc.add(Field('Title', str(title), contextType))
#         doc.add(Field('Text', str(text), contextType))
#         doc.add(Field('Author', str(author), metaType))
#         doc.add(Field('Score', int(score), metaType))
#         doc.add(Field('UpvoteRatio', int(upvote_ratio), metaType))
#         doc.add(Field('URL', str(url), metaType))
#         doc.add(Field('Permalink', str(permalink), metaType))
#         doc.add(Field('Comments', comments, contextType))
#         doc.add(Field('LinkTitles', link_titles, metaType))
#         doc.add(Field('CreatedAt', int(created_at), metaType))
#         writer.addDocument(doc)
#     writer.close()
    
# def retrieve(storedir, query):
#     if not lucene.getVMEnv():
#       lucene.initVM(vmargs=['-Djava.awt.headless=true'])
#     vm_env = lucene.getVMEnv()
#     vm_env.attachCurrentThread()
    
#     topkdocs = []
#     for directory in os.listdir(storedir):
#         path = os.path.join(storedir, directory)
#         if os.path.isdir(path):
#             searchDir = NIOFSDirectory(Paths.get(path))
#             searcher = IndexSearcher(DirectoryReader.open(searchDir))

#             parser = QueryParser('Text', StandardAnalyzer())
#             parsed_query = parser.parse(str(query))
            
#             topDocs = searcher.search(parsed_query, 10).scoreDocs

#             for hit in topDocs:
#                 doc = searcher.doc(hit.doc)
#                 topkdocs.append({
#                     "score": hit.score,
#                     "subreddit": doc.get("Subreddit"),
#                     "title": doc.get("Title"),
#                     "text": doc.get("Text"),
#                     "upvote": doc.get("Score"),
#                     "url": doc.get("URL"),
#                     "comments": doc.get("Comments"),
#                     "created": doc.get("CreatedAt")
#                 })

#     topkdocs = sorted(topkdocs, key=lambda x: x['score'], reverse=True)
    
#     return topkdocs[:11]

# # directory = 'DATA/'

# # for f in os.listdir(directory):
# #     path = os.path.join(directory, f)
# #     if os.path.isfile(path):
# #         create_index('reddit_index/', path)

# # query = input("Enter to Search: ")

# # results = retrieve('reddit_index/', query)
