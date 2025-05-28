from elasticsearch_dsl import Document, InnerDoc, Text, Keyword, Long, Date, Nested, Boolean
from elasticsearch_dsl.connections import connections
from elasticsearch.exceptions import ConnectionError

# Define the mapping for the 'files' field (for multi-file torrents)
class File(InnerDoc):
    path = Text(required=True, analyzer='ik_smart')
    length = Long(required=True)

# Define the mapping for the 'announce-list' field
class AnnounceURL(InnerDoc):
    url = Keyword(required=True)

# Define the main document mapping
class TorrentMetainfo(Document):
    infohash = Keyword(required=True)
    name = Text(required=True, analyzer='ik_smart')
    files = Nested(File)  # Optional, for multi-file torrents
    length = Long()  # Optional, for single-file torrents
    creation_date = Date()  # Optional
    encoding = Keyword()  # Optional
    announce_list = Nested(AnnounceURL)  # Optional
    source = Text(analyzer='ik_smart')  # Optional

    class Index:
        name = 'torrent_metainfo'

def main():
    # Define the connection to Elasticsearch
    connections.create_connection(hosts=['http://localhost:9200'], timeout=20)

    try:
        # Create the index with the mapping
        # The mapping is automatically applied if it doesn't exist
        if not TorrentMetainfo._index.exists():
            TorrentMetainfo.init()
            print(f"Index '{TorrentMetainfo._index.name}' created successfully with mapping.")
        else:
            print(f"Index '{TorrentMetainfo._index.name}' already exists.")

    except ConnectionError as e:
        print(f"Error connecting to Elasticsearch: {e}")
        print("Please ensure Elasticsearch is running and accessible.")

if __name__ == '__main__':
    main()
