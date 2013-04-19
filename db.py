import threading
import os
import pprint

lock = threading.Lock()

PATH='files.db'

S = '<sep>'
RS = ' <sep> '


tracks = {}
recent_tracks = []

def load():
    if os.path.exists(PATH):
        for i, f in enumerate(open(PATH)):
            fields = f.strip().split(RS)
            if len(fields) == 5:
                add_file(fields[0], fields[1], fields[2], fields[3], fields[4], flush=False, ready=True)
                

def get_track(trid):
    if trid in tracks:
        track = tracks[trid]
        return track
    else:
        return None

def get_url(trid):
    if trid in tracks:
        track = tracks[trid]
        return track['url']
    else:
        return None


def get_all():
    return recent_tracks


def get_recent(count):
    recent = []
    for t in reversed(recent_tracks):
        if t['ready']:
            recent.append(t)
            if len(recent) >= count:
                break
    return recent

def add_file(trid, artist, title, tag, url, flush=True, ready=False):
    # only allow mp3s in the directory

    if not url.lower().endswith('mp3'):
        return

    if trid not in tracks:
        track = {}
        artist = artist if artist else '(unknown artist)'
        title = title if title else '(unknown title)'

        track['id'] = trid
        track['artist'] = artist 
        track['title'] = title 
        track['tag'] = tag 
        track['url'] = url
        track['ready'] = ready

        tracks[trid] = track
        tracks[url] = track

        recent_tracks.append(track)

        if flush:
            save(track)

        return track

def track_normalize(tname):
    return tname.lstrip('0123456789 ')


def save(track):
    try:
        lock.acquire(True)
        out = open(PATH, 'a')
        print >>out,  RS.join( [track['id'], track['artist'], track['title'], track['tag'], track['url']] )
        print RS.join( [track['id'], track['artist'], track['title'], track['tag'], track['url']] )
        out.close()
    finally:
        lock.release()


if __name__ == '__main__':
    load()

