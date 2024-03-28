# TODO: clean up logging
# TODO: Add unit tests

PLUGIN_NAME = 'Resolve Artist Relations'
PLUGIN_AUTHOR = 'mmuffins'
PLUGIN_LICENSE = "GPL-2.0"
PLUGIN_LICENSE_URL = "https://www.gnu.org/licenses/gpl-2.0.txt"
PLUGIN_DESCRIPTION = 'Provides a new property containing a json with artist details and relations.'
# The patch version will be automatically replaced by the release action on github
PLUGIN_VERSION = '1.0.0' 
PLUGIN_API_VERSIONS = ['2.9', '2.10', '2.11', '3.0']

from picard import log
from picard.metadata import register_track_metadata_processor
from picard.webservice import ratecontrol
from picard.util import LockableObject
from functools import partial
from PyQt5.QtCore import QObject, pyqtSignal
import json


MAX_TRAVERSAL_DEPTH = 3
TRAVERSE_RELATION_TYPES_BLACKLIST = ['subgroup']
MB_DOMAIN = 'musicbrainz.org'
ratecontrol.set_minimum_delay(MB_DOMAIN, 1000) # 1 request per second

class WebrequestQueue(LockableObject):
    def __init__(self):
        LockableObject.__init__(self)
        self.queue = {}

    def __contains__(self, name):
        return name in self.queue

    def __getitem__(self, name):
        self.lock_for_read()
        try:
            return self.queue.get(name)
        finally:
            self.unlock()

    def __setitem__(self, name, value):
        self.lock_for_write()
        try:
            self.queue[name] = value
        finally:
            self.unlock()

    def hasTrack(self, album, track):
      for artistId in self.queue:
        queueItems = self.queue[artistId]
        if any(sublist[0].id == album.id and sublist[1]['id'] == track['id'] for sublist in queueItems if len(sublist) > 1):
          return True
      return False

    def append(self, name, value):
        self.lock_for_write()
        try:
            if name in self.queue:
                queueItems = self.queue[name]
                if not any(sublist[0].id == value[0].id and sublist[1]['id'] == value[1]['id'] for sublist in queueItems if len(sublist) > 1):
                  # Only enqueue a new item if the album and track id doesn't already exist in the queue
                  self.queue[name].append(value)
                
                value = False
            else:
                self.queue[name] = [value]
                value = True
            return value
        finally:
            self.unlock()

    def remove(self, name):
      self.lock_for_write()
      value = None
      try:
        if name in self.queue:
          value = self.queue[name]
          del self.queue[name]
        return value
      finally:
        self.unlock()

class Relation:
  def __init__(self, relation_data):
    self.artist = None
    self.direction = relation_data.get('direction', '')
    self.targetType = relation_data.get('target-type', '')
    self.type = relation_data.get('type', '')
    self.id = self.get_target_id(relation_data)

  def get_target_id(self, relation_data):
    if 'artist' in relation_data:
      return relation_data['artist']['id']
    
    log.error(f"Relation of type {self.type} with target type {self.targetType} has no artist property.")
    return ''

class Artist:
  def __init__(self, name='', type_='', disambiguation='', sort_name='', id_='', aliases=None, type_id='', relations=None):
    self.name = name
    self.type = type_
    self.disambiguation = disambiguation
    self.sort_name = sort_name
    self.id = id_
    self.aliases = aliases if aliases is not None else []
    self.type_id = type_id
    self.relations = self.process_relations(relations)

  @staticmethod
  def create(artist_data):
    name = artist_data.get('name', '')
    type_ = artist_data.get('type', '')
    disambiguation = artist_data.get('disambiguation', '')
    sort_name = artist_data.get('sort-name', '')
    id_ = artist_data.get('id', '')
    aliases = artist_data.get('aliases', [])
    type_id = artist_data.get('type-id', '')
    relations = artist_data.get('relations', [])

    return Artist(name, type_, disambiguation, sort_name, id_, aliases, type_id, relations)

  def process_relations(self, relations):
    result = []
    if relations is None:
      return result
    
    for relation in relations:
      if relation['direction'].lower() != 'backward':
        continue

      if relation['type'] in TRAVERSE_RELATION_TYPES_BLACKLIST:
        continue

      result.append(Relation(relation))
    
    return result

  def to_dict(self, artistCache):
    return {
      "name": self.name,
      "type": self.type,
      "disambiguation": self.disambiguation,
      "sort_name": self.sort_name,
      "id": self.id,
      "aliases": self.aliases,
      "type_id": self.type_id,
      "relations": [artistCache[relation.id].to_dict(artistCache) for relation in self.relations]
    }


class ArtistResolver(QObject):
  finished = pyqtSignal(object)  # Signal to indicate all web requests are done
  
  # Shared cache and lock for thread-safe access
  artist_queue = WebrequestQueue()
  artist_cache = {}

  def __init__(self):
    super().__init__()

  def get_track_artists(self, album, track):
    result = []
    for credit in track['artist-credit']:
      if ('artist' in credit):
        result.append(credit['artist']['id'])
    return result

  def is_artist_resolved(self, artist):
    if artist is None:
      return False
    
    for relation in artist.relations:
      unresolved = self.is_artist_resolved(relation.artist)
      if unresolved is False:
        return False
    
    return True

  def serialize_track_artists(self, album, track):
    trackArtists = []
    for credit in track['artist-credit']:
      artistObj = self.artist_cache[credit['artist']['id']].to_dict(self.artist_cache)
      artistObj['joinphrase'] = credit['joinphrase']
      trackArtists.append(artistObj)

    return json.dumps(trackArtists, ensure_ascii=False)
  
  def all_artists_resolved(self, album, track):
    result = []
    track_artist_ids = self.get_track_artists(album, track)
    for artistId in track_artist_ids:
      result.append(self.get_artist_relations(album, track, artistId))
    
    for artist in result:
        if False == self.is_artist_resolved(artist):
            return False

    return True

  def resolve_artists(self, album, track):
    if self.artist_queue.hasTrack(album, track):
      # Only proceed to check if all artists are resolved if no artists for this track are in the lookup queue
      log.debug(f"resolve_artists {track['title']}: skipping due to open items in queue")
      return

    log.debug(f"resolve_artists {track['title']}")

    if False == self.all_artists_resolved(album, track):
        return False
    
    log.debug(f"Finished resolving artists for track {track['title']} in {album.id}")

    resolved_artists = self.serialize_track_artists(album, track)
    self.finished.emit(resolved_artists)
  
  def get_artist_relations(self, album, track, artistId):
    log.debug(f"get_artist_relations {track['title']}, {artistId}")

    result = None
    if (artistId not in self.artist_cache):
      self.get_artist_details(album, track, artistId)
      return result
    
    result = self.artist_cache[artistId]
    for relation in result.relations:
      if relation.artist != []:
        relation.artist = self.get_artist_relations(album, track, relation.id)
    return result

  def get_artist_details(self, album, track, artistId):
    log.debug(f"get_artist_details {track['title']}, {artistId}")

    url = f"https://{MB_DOMAIN}/ws/2/artist/{artistId}/?inc=artist-rels+aliases&fmt=json"

    if self.artist_queue.append(artistId, (album, track, self)):
      log.debug(f"call webrequest self:{id(self)} {track['title']}, {artistId}")
      album.tagger.webservice.get_url(url=url, handler=partial(self.process_artist_request_response, artistId))

  def process_artist_request_response(self, artistId, response, reply, error):
    if error:
      log.error("Error fetching artist details: %s", error)
      return
    
    log.debug(f"process_artist_request_response self:{id(self)}, {artistId}")

    artist = Artist.create(response)
    self.artist_cache[artistId] = artist
    resolveTracks = self.artist_queue.remove(artistId)

    for album, track, resolver in resolveTracks:
      log.debug(f"call resolve_artists ({artistId}) for  {track['title']} ")
      resolver.resolve_artists(album, track)

def track_artist_processor(album, metadata, track, release):
  log.debug(f"start processing {track['title']}, requests: {album._requests}")
  resolver = ArtistResolver()
  album._requests += 1
  resolver.finished.connect(lambda resolved_artists: track_finished(resolved_artists, metadata, track, album))
  resolver.resolve_artists(album, track)

def track_finished(resolved_artists, metadata, track, album):
  log.debug(f"process_finished {track['title']} in {album.id}")

  metadata['artist_relations_json'] = resolved_artists
  album._requests -= 1

  log.debug(f"albumrequests count {album._requests}") 
  # Workaround for an endless loop where no artist data needs to be retrieved
  # If finalize_loading is called before tracks are loaded, which is likely
  # to happen if the plugin doesn't need to load any data and therefore finishes immediately,
  # it will try to load the tracks again, which calls register_track_metadata_processor,
  # triggering an endless loop
  # I can't also remove it since it's needed for long running relation lookups because it tells
  # the application that everything is finished
  if album._tracks_loaded:
    album._finalize_loading(None)


register_track_metadata_processor(track_artist_processor)