# TODO: Limit traversal depth
# TODO: fix get_included_artists only returning root artist
# TODO: Check how to get translations


PLUGIN_NAME = 'Resolve Character Artists'
PLUGIN_AUTHOR = 'Your Name'
PLUGIN_DESCRIPTION = 'Resolves fictional characters to real artists.'
PLUGIN_VERSION = '0.2'
PLUGIN_API_VERSIONS = ['2.0', '2.1', '2.2', '2.3', '2.4', '2.5', '2.6', '2.9', '2.10', '2.11', '3.0']

import threading
from picard import log
from picard.metadata import register_track_metadata_processor
from picard.webservice import ratecontrol
from picard.util import LockableObject
from functools import partial
from PyQt5.QtCore import QObject, pyqtSignal


MAX_TRAVERSAL_DEPTH = 3
TRAVERSE_RELATION_TYPES_BLACKLIST = ['subgroup', 'Person']
MB_DOMAIN = 'musicbrainz.org'
ratecontrol.set_minimum_delay(MB_DOMAIN, 2000) # 0.5 requests per second

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

    def append(self, name, value):
        self.lock_for_write()
        try:
            if name in self.queue:
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

class Artist:
  def __init__(self, artistResolver, album, name='', type_='', disambiguation='', sort_name='', id_='', aliases=None, type_id='', joinphrase='', relation_direction='', relation_target_type='', relation_type_id='', relation_type=''):
    self.name = name
    self.artistResolver = artistResolver
    self.artistCache = ArtistResolver.artist_cache
    self.webrequestQueue = ArtistResolver.artist_queue
    self.album = album
    self.type = type_
    self.disambiguation = disambiguation
    self.sort_name = sort_name
    self.id = id_
    self.aliases = aliases if aliases is not None else []
    self.type_id = type_id
    self.joinphrase = joinphrase
    self.relation_direction = relation_direction
    self.relation_target_type = relation_target_type
    self.relation_type_id = relation_type_id
    self.relation_type = relation_type
    self.relations = None
    self.Include = True

    self.update_type_properties()

  def update_relations(self, artist_data):
    self.disambiguation = artist_data.get('disambiguation', self.disambiguation)
    self.sort_name = artist_data.get('sort-name', self.sort_name)
    self.aliases = artist_data.get('aliases', self.aliases)
    self.joinphrase = artist_data.get('joinphrase', self.joinphrase)
    # Process relations
    self.relations = []
    
    # only fetch backward relations to prevent getting into relation loops
    for relation in [rel for rel in artist_data.get('relations', []) if rel.get('direction') == 'backward']:
      relation_artist_data = relation['artist']
      relation_artist_data['relation_type'] = relation.get('type')
      relation_artist_data['relation_direction'] = relation.get('direction')
      relation_artist_data['relation_target_type'] = relation.get('target-type')
      relation_artist_data['relation_type_id'] = relation.get('type-id')
      relation_artist_data['relation_type'] = relation.get('type')
      relation_artist = Artist.create(self.artistResolver, self.album, relation_artist_data)
      self.relations.append(relation_artist)

  @staticmethod
  def create(artistResolver, album, artist_data):
    name = artist_data.get('name', '')
    joinphrase = artist_data.get('joinphrase', '')
    artist_info = artist_data.get('artist', {}) if 'artist' in artist_data else artist_data
    type_ = artist_info.get('type', '')
    disambiguation = artist_info.get('disambiguation', '')
    sort_name = artist_info.get('sort-name', '')
    id_ = artist_info.get('id', '')
    aliases = artist_info.get('aliases', [])
    type_id = artist_info.get('type-id', '')
    relation_direction = artist_info.get('relation_direction', '')
    relation_target_type = artist_info.get('target_type', '')
    relation_type_id = artist_info.get('relation_type_id', '')
    relation_type = artist_info.get('relation_type', '')

    return Artist(artistResolver, album, name, type_, disambiguation, sort_name, id_, aliases, type_id, joinphrase, relation_direction, relation_target_type, relation_type_id, relation_type)

  def update_type_properties(self):
    if self.type == "Person":
      self.Include = True
      self.relations = []
    
    if self.relation_type in TRAVERSE_RELATION_TYPES_BLACKLIST:
      self.relations = []

  def get_unresolved_artists(self, result = None):
    if result is None:
      result = []

    if self.relations is None:
      result.append(self)
      return result

    for artist in self.relations:
      artist.get_unresolved_artists(result),

    return result
  
  def get_included_artists(self, result = None):
    if result is None:
      result = []
    
    if self.Include is True:
      result.append(self)

    if self.relations is None:
      return result
    
    for artist in self.relations:
      artist.get_unresolved_artists(result)

    return result

  def resolve_relations(self):
    unresolved = self.get_unresolved_artists()

    if not unresolved:
      self.artistResolver.resolve_artists()
      return
    
    unresolved[0].fetch_relations()

  def fetch_relations(self):
    url = f"https://{MB_DOMAIN}/ws/2/artist/{self.id}/?inc=artist-rels&fmt=json"

    if self.id in self.artistCache:
      self.update_relations(self.artistCache[self.id])
      self.resolve_relations()
    else:
      if self.webrequestQueue.append(self.id, self):
        self.album.tagger.webservice.get_url(url=url, handler=partial(self.process_artist_relations_response, self.id, self.album))

  def process_artist_relations_response(self, artistId, album, response, reply, error):
    if error:
      log.error("Error fetching artist details: %s", error)
      return
    
    self.artistCache[artistId] = response
    checkTracks = self.webrequestQueue.remove(artistId)

    for artist in checkTracks:
      ddd(artist.artistResolver)


class ArtistResolver(QObject):
  finished = pyqtSignal(object)  # Signal to indicate all web requests are done
  
  # Shared cache and lock for thread-safe access
  artist_cache = {}
  artist_queue = WebrequestQueue()

  def __init__(self, album, track, artists):
    super().__init__()
    self.album = album
    self.track = track
    self.artists = self.process_artists(artists)


  def process_artists(self, artistCredit):
    processed = []

    for credit in artistCredit:
      if ('type' in credit['artist'] and credit['artist']['type'].lower() == 'character') and ('joinphrase' in credit and 'cv' in credit['joinphrase'].lower()):
        continue

      processed.append(Artist.create(self, self.album, credit))
    return processed

  def get_included_artists(self):
    result = []
    for artist in self.artists:
      result.extend(artist.get_included_artists())

    return result

  def get_unresolved_artists(self):
    result = []
    for artist in self.artists:
      result.extend(artist.get_unresolved_artists())
    
    return result

  def resolve_artists(self):
    unresolved_artists = self.get_unresolved_artists()
    
    if not unresolved_artists:
      log.debug(f"no unprocessed artists left to traverse ({self.track['title']})")
      included_artists = self.get_included_artists()
      resolved_artists_string = '; '.join([artist.name for artist in included_artists])
      self.finished.emit(resolved_artists_string)
      return
    
    unresolved_artists[0].resolve_relations()

# def process_artists(artistCredit, sourceRelation = None):
#   processed = []
#   procX = []

#   for credit in artistCredit:
#     procX.append(Artist.create_from_data(credit))
#     artist = flatten_artist_credit(credit, sourceRelation)
    
#     if artist['type'] == 'Person':
#       # artist['resolve'] = True
#       artist['relations'] = []

#     if 'relation-type' in artist and artist['relation-type'] in TRAVERSE_RELATION_TYPES_BLACKLIST:
#       # artist['resolve'] = False
#       artist['relations'] = []

#     processed.append(artist)
    
#   return processed

# def flatten_artist_credit(credit, sourceRelation = None):
#   artist = credit
#   if 'artist' in credit:
#     artist = credit['artist']
    
#   # if sourceRelation is None: 
#   #   artist['relation-source'] = None
#   #   artist['relation-target-credit'] = None
#   #   artist['relation-direction'] = None
#   #   artist['relation-type-id'] = None
#   #   artist['relation-target-type'] = None
#   #   artist['relation-type'] = None
#   #   artist['relation-source-credit'] = None
#   # else:
#   #   artist['relation-source'] = sourceRelation
#   #   artist['relation-target-credit'] = credit['target-credit']
#   #   artist['relation-direction'] = credit['direction']
#   #   artist['relation-type-id'] = credit['type-id']
#   #   artist['relation-target-type'] = credit['target-type']
#   #   artist['relation-type'] = credit['type']
#   #   artist['relation-source-credit'] = credit['source-credit']

#   if 'include' not in artist:
#     artist['include'] = True

#   # if 'resolve' not in artist:
#   #     artist['resolve'] = True
#   return artist

def ddd(resolver):
  resolver.resolve_artists()

def process_finished(resolved_artists, metadata, track, album):
  log.debug(f"process_finished ({track['title']})")

  metadata['resolved_artists'] = resolved_artists
  album._requests -= 1

  log.debug(f"albumrequests count {album._requests}") 

  # Workaround for an endless loop where no artist data needs to be retrieved
  # If finalize_loading is called before tracks are loaded, which is likely
  # to happen if the plugin doesn't need to load any data and therefore finishes immediatly,
  # it will try to load the tracks again, which calls register_track_metadata_processor,
  # triggering an endless loop
  # I can't also remove it since it's needed for long running relation lookups because it tells
  # the application that everything is finished
  if album._tracks_loaded:
    album._finalize_loading(None)

def track_artist_processor(album, metadata, track, release):
    log.debug(f"start processing {track['title']}, requests: {album._requests}")
    # log.debug(f"requests:1 {album._requests}")
    album._requests += 1
    resolver = ArtistResolver(album, track, track['artist-credit'])
    resolver.finished.connect(lambda resolved_artists: process_finished(resolved_artists, metadata, track, album))
    ddd(resolver)
    # log.debug(f"requests:2 {album._requests}")


register_track_metadata_processor(track_artist_processor)